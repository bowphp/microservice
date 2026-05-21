# bowphp/microservice — User Guide

A NestJS-style microservice layer for BowPHP. Write controllers with attributes,
run a consumer that listens on a transport, call them with a `ClientProxy`.
Five transports share one envelope so the same handler runs unchanged over
TCP, Redis, RabbitMQ, Kafka, or gRPC.

> French: see [docs/fr.md](fr.md).

## Table of contents

- [bowphp/microservice — User Guide](#bowphpmicroservice--user-guide)
  - [Table of contents](#table-of-contents)
  - [Concepts](#concepts)
  - [Installation](#installation)
  - [Quick start](#quick-start)
  - [Defining handlers](#defining-handlers)
  - [Running the consumer](#running-the-consumer)
  - [Calling from another service](#calling-from-another-service)
  - [Transports in detail](#transports-in-detail)
    - [TCP](#tcp)
    - [Redis](#redis)
    - [RabbitMQ](#rabbitmq)
    - [Kafka](#kafka)
    - [gRPC](#grpc)
  - [Configuration](#configuration)
  - [BowPHP integration](#bowphp-integration)
  - [Custom serializer](#custom-serializer)
  - [Errors and logging](#errors-and-logging)
  - [Adding a new transport](#adding-a-new-transport)
  - [Limits and notes](#limits-and-notes)

## Concepts

**Packet.** Every transport carries the same envelope: `{ pattern, data, id, kind }`.

- `pattern` — string the consumer dispatches on, e.g. `"user.find"`.
- `data` — JSON-serialisable payload.
- `id` — correlation id (empty for events).
- `kind` — `"message"` (RPC) or `"event"` (fire-and-forget).

**ResponsePacket.** RPC reply: `{ id, response, isDisposed, err }`. The `id` matches the request packet so the client can correlate.

**Pattern dispatch.** A consumer registers controllers; the `HandlerRegistry`
uses reflection to map every method annotated with `#[MessagePattern('foo')]` or
`#[EventPattern('bar')]` to its pattern string. When a packet arrives, the
server looks the pattern up and invokes the method with `(mixed $data, Packet $packet)`.

**Transport seams.** Two contracts — `ServerTransport` and `ClientTransport` —
hide protocol details. Swapping transports requires no handler change.

## Installation

```bash
composer require bowphp/microservice
```

Per-transport requirements:

| Transport | Requirement                                                       |
|-----------|-------------------------------------------------------------------|
| TCP       | `ext-sockets` (built into most PHP builds)                        |
| Redis     | `ext-redis` (phpredis)                                            |
| RabbitMQ  | `php-amqplib/php-amqplib` (Composer dependency, already required) |
| Kafka     | `ext-rdkafka`                                                     |
| gRPC      | `pecl install grpc && composer require grpc/grpc google/protobuf` |

## Quick start

**Controller** — plain PHP class with attributed methods.

```php
namespace App\Consumers;

use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Message\Packet;

final class UserConsumer
{
    #[MessagePattern('user.find')]
    public function find(mixed $data): array
    {
        $id = (int) ($data['id'] ?? 0);
        return ['id' => $id, 'name' => "User #{$id}"];
    }

    #[EventPattern('user.created')]
    public function onCreated(mixed $data, Packet $packet): void
    {
        // send welcome email, write audit log…
    }
}
```

**Register the controller** in `config/microservice.php`:

```php
'controllers' => [
    \App\Consumers\UserConsumer::class,
],
```

**Run the consumer**:

```bash
php bow microservice:listen --transport=redis
```

**Call it from another service**:

```php
use Bow\Microservice\Client\ClientFactory;

$proxy = ClientFactory::create('redis', ['host' => '127.0.0.1']);
$proxy->connect();

$user = $proxy->send('user.find', ['id' => 42]);  // RPC — blocks for reply
$proxy->emit('user.created', ['id' => 99]);       // event — returns immediately
```

## Defining handlers

A controller is just a class. The library finds handlers via reflection.

```php
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Message\Packet;

final class OrderConsumer
{
    public function __construct(private OrderService $orders) {}

    #[MessagePattern('order.create')]
    public function create(mixed $data): array
    {
        $order = $this->orders->create($data);
        return ['id' => $order->id, 'total' => $order->total];
    }

    #[MessagePattern('order.find')]
    public function find(mixed $data, Packet $packet): ?array
    {
        // $packet carries the correlation id, pattern, kind — useful for tracing
        return $this->orders->find((int) ($data['id'] ?? 0))?->toArray();
    }

    #[EventPattern('order.paid')]
    public function onPaid(mixed $data): void
    {
        $this->orders->markPaid((int) $data['id']);
    }
}
```

Key rules:

- The first parameter receives the decoded payload (`mixed`).
- The second parameter (optional) receives the raw `Packet`.
- RPC handlers (`#[MessagePattern]`) return any JSON-serialisable value; that becomes the `response` field of the `ResponsePacket`.
- Event handlers (`#[EventPattern]`) have no return value.
- Constructor DI is available when running through Bow (controllers are resolved through the container).

## Running the consumer

The package registers a Bow console command, `microservice:listen`:

```bash
# Use config/microservice.php for everything
php bow microservice:listen

# Override the transport on the command line
php bow microservice:listen --transport=tcp --host=0.0.0.0 --port=3000

# Restrict the controllers loaded
php bow microservice:listen --controllers="App\Consumers\UserConsumer,App\Consumers\OrderConsumer"
```

CLI flags override `config/microservice.php`. Available flags:

| Flag             | Used by                | Default                              |
|------------------|------------------------|--------------------------------------|
| `--transport`    | all                    | `config('microservice.transport')`   |
| `--controllers`  | all                    | `config('microservice.controllers')` |
| `--host`         | tcp / redis / rabbitmq | transport-specific                   |
| `--port`         | tcp / redis / rabbitmq | transport-specific                   |
| `--password`     | redis / rabbitmq       | none                                 |
| `--patterns`     | redis                  | empty                                |
| `--queue`        | rabbitmq               | `bow_microservice`                   |
| `--user`         | rabbitmq               | `guest`                              |
| `--vhost`        | rabbitmq               | `/`                                  |
| `--topics`       | kafka                  | empty                                |
| `--brokers`      | kafka                  | `127.0.0.1:9092`                     |
| `--group`        | kafka                  | `bow-microservice`                   |

Run multiple instances behind a load balancer / supervisor for concurrency.
Each transport handles one connection at a time per process (see
[Limits and notes](#limits-and-notes)).

**Graceful shutdown.** When `ext-pcntl` is available the command installs
SIGTERM / SIGINT handlers that call `$server->stop()` before exiting with
status `0`, so supervisord / systemd / Kubernetes can drain a consumer
cleanly. Without `ext-pcntl` the process just exits on signal as usual.

## Calling from another service

Use `ClientFactory::create` to build a `ClientProxy`:

```php
use Bow\Microservice\Client\ClientFactory;

$proxy = ClientFactory::create('redis', [
    'host' => '127.0.0.1',
    'port' => 6379,
]);
$proxy->connect();

// RPC — blocks until reply or timeout
$user = $proxy->send('user.find', ['id' => 42]);

// Event — fire and forget
$proxy->emit('user.created', ['id' => 99]);
```

If the consumer raised an exception, `send()` re-throws it as
`Bow\Microservice\Exception\RpcException`. Events swallow handler errors and
log them server-side (no client to notify).

Inside a Bow app, the same proxy is auto-wired in the container — see
[BowPHP integration](#bowphp-integration).

## Transports in detail

### TCP

Length-prefixed JSON frames (4-byte big-endian length + payload) over a raw
socket. No broker required.

```bash
php bow microservice:listen --transport=tcp --host=0.0.0.0 --port=3000
```

```php
$proxy = ClientFactory::create('tcp', ['host' => '127.0.0.1', 'port' => 3000]);
```

**Limits.** One connection at a time per process. Production: run N workers
behind a TCP balancer (HAProxy, nginx stream).

### Redis

Uses `phpredis`. Requests use pub/sub (one channel per registered pattern);
replies use a per-request Redis list keyed by the packet `id`
(`bow:reply:<id>`). The server `RPUSH`es the reply onto that list, the client
`BLPOP`s it. Lists queue items, so there's no publish-before-subscribe race —
even a server that replies before the client is ready to read won't drop
messages.

```bash
php bow microservice:listen --transport=redis --patterns=user.find,user.created
```

```php
$proxy = ClientFactory::create('redis', ['host' => '127.0.0.1']);
```

The `--patterns` flag tells the consumer which Redis channels to subscribe to.
Without it, the consumer cannot receive anything — it doesn't auto-discover.

### RabbitMQ

Durable queue-based messaging via `php-amqplib`. RPC uses AMQP's `reply_to`
and `correlation_id` mechanism.

```bash
php bow microservice:listen --transport=rabbitmq \
    --queue=bow_microservice --user=guest --password=guest
```

```php
$proxy = ClientFactory::create('rabbitmq', [
    'queue' => 'bow_microservice',
    'host'  => '127.0.0.1',
]);
```

The queue is declared automatically on first connection.

### Kafka

High-throughput streaming with `rdkafka`. Mirrors NestJS's approach: a
request topic for incoming work, a reply topic correlated by a
`kafka_correlationId` header.

```bash
php bow microservice:listen --transport=kafka \
    --topics=user_events --group=users
```

```php
$proxy = ClientFactory::create('kafka', [
    'brokers' => '127.0.0.1:9092',
    'topic'   => 'user_events',
]);
```

Kafka has no native RPC; the reply-topic approach matches Nest. For pure
event streaming, use `emit()` only.

### gRPC

**Client only.** PHP has no native gRPC server, so implement the server in any
language that does (Go, Node, Rust, Java) following
[proto/microservice.proto](../proto/microservice.proto). The proto defines one
service:

```proto
service MessageService {
  rpc Send(MessageEnvelope) returns (MessageEnvelope);
  rpc Emit(MessageEnvelope) returns (Empty);
}

message MessageEnvelope {
  bytes payload = 1;   // JSON-encoded Packet / ResponsePacket
}
```

The `payload` field carries the exact same JSON bytes the other transports use,
so your existing handlers (running on Go/Node/etc.) keep dispatching by pattern
string.

```php
$proxy = ClientFactory::create('grpc', [
    'host' => '127.0.0.1',
    'port' => 50051,
]);
$user = $proxy->send('user.find', ['id' => 42]);
```

Throws `TransportException` on `connect()` if the `grpc` extension is missing.

## Configuration

`config/microservice.php` is the single source of truth:

```php
return [
    'transport' => app_env('MICROSERVICE_TRANSPORT', 'redis'),
    'timeout'   => (float) app_env('MICROSERVICE_TIMEOUT', 5.0),

    'controllers' => [
        \App\Consumers\UserConsumer::class,
        \App\Consumers\OrderConsumer::class,
    ],

    'tcp'      => ['host' => '127.0.0.1', 'port' => 3000],
    'redis'    => ['host' => '127.0.0.1', 'port' => 6379, 'password' => null],
    'rabbitmq' => ['host' => '127.0.0.1', 'port' => 5672, 'user' => 'guest', 'password' => 'guest', 'queue' => 'bow_microservice'],
    'kafka'    => ['brokers' => '127.0.0.1:9092', 'topic' => 'bow_microservice'],
    'grpc'     => ['host' => '127.0.0.1', 'port' => 50051],
];
```

Without this file the provider falls back to environment variables:
`MICROSERVICE_TRANSPORT`, `MICROSERVICE_HOST`, `MICROSERVICE_PORT`,
`MICROSERVICE_QUEUE`, `MICROSERVICE_BROKERS`, etc.

## BowPHP integration

Register the configuration provider once:

```php
// app/Kernel.php
public function configurations(): array
{
    return [
        \Bow\Microservice\Bow\MicroserviceConfiguration::class,
    ];
}
```

Bindings exposed in the container:

- `Bow\Microservice\Client\ClientProxy::class` — type-hint in controllers / services.
- `'microservice.client'` — string alias for `app('microservice.client')`.

The proxy is connected eagerly at boot so a misconfigured transport fails
immediately instead of mid-request.

Inside a Bow controller:

```php
use Bow\Microservice\Client\ClientProxy;

final class CheckoutController
{
    public function __construct(private ClientProxy $microservice) {}

    public function pay(int $orderId): Response
    {
        $this->microservice->emit('order.paid', ['id' => $orderId]);
        return response()->json(['ok' => true]);
    }
}
```

Consumer controllers also resolve through the container, so they can use
constructor DI exactly like HTTP controllers.

## Custom serializer

The default `JsonSerializer` is enough for most cases. To use msgpack /
Protobuf / etc., implement `Bow\Microservice\Contracts\Serializer`:

```php
use Bow\Microservice\Contracts\Serializer;

final class MsgpackSerializer implements Serializer
{
    public function encode(mixed $value): string
    {
        return msgpack_pack($value);
    }

    public function decode(string $payload): mixed
    {
        return msgpack_unpack($payload);
    }
}
```

Pass it explicitly when building a transport (the factories take a JSON
serializer by default; instantiate the transport directly to inject another).

## Errors and logging

**RPC.** A handler exception becomes an error `ResponsePacket`; the client
re-throws it as `Bow\Microservice\Exception\RpcException` with the original
message. The exception type is lost (different runtimes); use error codes
inside the payload if you need typed dispatching.

**Events.** No caller to notify, so handler errors are swallowed and logged
through the optional PSR-3 logger passed to `MicroserviceFactory::create`.

**Transport errors.** Connection problems, malformed packets, etc. raise
`Bow\Microservice\Exception\TransportException`. The console command catches
these and prints a clear message before exiting non-zero.

## Adding a new transport

A new transport is two classes:

```php
namespace App\Transport;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

final class NatsServerTransport implements ServerTransport
{
    public function listen(callable $onPacket): void { /* … */ }
    public function reply(Packet $request, ResponsePacket $response): void { /* … */ }
    public function stop(): void { /* … */ }
    public function name(): string { return 'nats'; }
}

final class NatsClientTransport implements ClientTransport
{
    public function connect(): void { /* … */ }
    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket { /* … */ }
    public function emit(Packet $packet): void { /* … */ }
    public function close(): void { /* … */ }
    public function name(): string { return 'nats'; }
}
```

Register them in your own factory, or add cases to `MicroserviceFactory` and
`ClientFactory` if upstreaming.

## Limits and notes

- **Single connection per consumer process.** All server transports are
  blocking and handle one packet at a time. Run N copies behind a load
  balancer for concurrency.
- **Kafka has no native RPC.** The reply-topic + correlation-id approach
  works but introduces extra latency. Prefer `emit()` for high-throughput
  event flows.
- **gRPC is client-only.** Server-side gRPC in PHP requires RoadRunner or
  Swoole, neither of which matches the single-process consumer model.
- **JSON serializer by default.** Implement `Serializer` to switch to a
  binary format if payload size matters.
- **Handler exceptions are flattened.** Type information is lost across the
  wire; encode error semantics inside the payload if you need typed errors.
