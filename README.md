# bowphp/microservice

A NestJS-style, multi-transport microservice layer for **BowPHP**. One handler
API, five transports: **TCP**, **Redis** (pub/sub), **RabbitMQ** (AMQP),
**Kafka**, and **gRPC** (client only).

The design mirrors NestJS: you write *controllers* whose methods carry
`#[MessagePattern]` (request/response) or `#[EventPattern]` (fire-and-forget)
attributes, then run a consumer that listens on a transport. Callers use a
`ClientProxy` with `send()` / `emit()`. The transport only moves bytes — your
handlers never change when you switch protocols.

> 📖 Full user guide: [docs/en.md](docs/en.md) — French version: [docs/fr.md](docs/fr.md)

## Architecture

The package is a **framework-agnostic core** with a thin BowPHP adapter:

```
Contracts/         ServerTransport, ClientTransport, Serializer  (the seams)
Message/           Packet, ResponsePacket, JsonSerializer        (wire format)
Consumer/          MicroserviceServer, HandlerRegistry,
                   MessagePattern, EventPattern, MicroserviceFactory
Client/            ClientProxy, ClientFactory
Transport/         {Tcp,Redis,RabbitMq,Kafka}{Server,Client}Transport
                   GrpcClientTransport + Grpc/ (envelope, stub, codec)
Bow/               MicroserviceConfiguration  (the ONLY Bow-coupled file)
```

Every transport implements the same two contracts, so adding a sixth (NATS,
MQTT, etc.) means writing one server + one client class — nothing else moves.

### The envelope

All transports share one packet shape (like Nest's `{ pattern, data, id }`):

- **RPC**: request `Packet` → handler return value → `ResponsePacket`, matched
  back to the caller by `id`.
- **Event**: `Packet` with no reply.

How each transport correlates replies:

| Transport | RPC reply mechanism                                  |
|-----------|------------------------------------------------------|
| TCP       | same socket, 4-byte length-prefixed JSON frames      |
| Redis     | `pattern` channel → `pattern.reply` channel + id     |
| RabbitMQ  | `reply_to` queue + `correlation_id`                  |
| Kafka     | reply topic + `kafka_correlationId` header           |
| gRPC      | unary `MessageService.Send` returns MessageEnvelope  |

## Install

```bash
composer require bowphp/microservice
```

Then install the extension/library for the transport(s) you use:

- TCP → `ext-sockets` (built in on most PHP)
- Redis → `ext-redis` (phpredis)
- RabbitMQ → `composer require php-amqplib/php-amqplib`
- Kafka → `ext-rdkafka`
- gRPC (client) → `pecl install grpc && composer require grpc/grpc google/protobuf`

## Define handlers

```php
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Message\Packet;

final class UserController
{
    #[MessagePattern('user.find')]
    public function find(mixed $data, Packet $packet): array
    {
        return ['id' => $data['id'], 'name' => "User #{$data['id']}"];
    }

    #[EventPattern('user.created')]
    public function onCreated(mixed $data): void
    {
        // send welcome email, etc.
    }
}
```

## Run the consumer

Two equivalent entrypoints — the BowPHP-integrated console command (recommended)
and a standalone script:

```bash
# BowPHP console command — registered automatically by MicroserviceConfiguration
php bow microservice:listen --transport=redis    --patterns=user.find,user.created
php bow microservice:listen --transport=tcp      --host=0.0.0.0 --port=3000
php bow microservice:listen --transport=rabbitmq --queue=bow_microservice
php bow microservice:listen --transport=kafka    --topics=user_events --group=users

# Standalone script — no Bow integration required
php examples/microservice.php --transport=redis --patterns=user.find
```

Both honour `config/microservice.php`; CLI flags override config. They install
SIGTERM/SIGINT handlers (when `ext-pcntl` is available) so supervisord /
systemd / Kubernetes can drain a worker cleanly. Run N copies for concurrency.

## Call from another service

```php
use Bow\Microservice\Client\ClientFactory;

$proxy = ClientFactory::create('redis', ['host' => '127.0.0.1']);
$proxy->connect();

$user = $proxy->send('user.find', ['id' => 42]); // RPC, blocks for reply
$proxy->emit('user.created', ['id' => 99]);       // fire-and-forget

// gRPC client to a non-PHP server implementing proto/microservice.proto
$grpc = ClientFactory::create('grpc', ['host' => '127.0.0.1', 'port' => 50051]);
$grpc->connect();
$grpc->send('user.find', ['id' => 42]);
```

## BowPHP integration

Register the configuration provider in your Kernel so a connected `ClientProxy`
is bound in the container:

```php
// app/Kernel.php
public function configurations(): array
{
    return [
        \Bow\Microservice\Bow\MicroserviceConfiguration::class,
    ];
}
```

Add `config/microservice.php` to your app (see this repo's
[`config/microservice.php`](config/microservice.php) for the template). The
provider binds both `ClientProxy::class` and the `microservice.client` alias.

Both consumer entrypoints (`php bow microservice:listen` and
`php examples/microservice.php`) boot the kernel and instantiate controllers
through Bow's container — so your consumers can use constructor DI just like
HTTP controllers. List them in `config('microservice.controllers')` or pass
`--controllers=A,B` on the CLI. The standalone script also accepts
`--kernel=` (or `MICROSERVICE_KERNEL`) to point at a custom Kernel class.

## Notes & limits

- The TCP server handles one connection at a time per process — run multiple
  workers behind a balancer, or swap in an event-loop driver later (the framing
  is unchanged).
- Kafka has no native RPC; the reply-topic approach matches NestJS. For pure
  event streaming, use `emit()` only.
- The default serializer is JSON. Implement `Serializer` for msgpack/protobuf.
- Handler exceptions become an error `ResponsePacket`; the client re-throws them
  as `RpcException`. Events swallow-and-log errors (no caller to notify).
- **gRPC is client-only.** The `grpc` PHP extension provides no server API and
  a production-grade PHP gRPC server requires RoadRunner or Swoole, neither of
  which fits the single-process consumer model here. Implement the server side
  in any language with a real gRPC server following [proto/microservice.proto](proto/microservice.proto) —
  PHP services call it through `GrpcClientTransport`.
