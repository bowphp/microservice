# bowphp/microservice

A NestJS-style, multi-transport microservice layer for **BowPHP**. One handler
API, four transports: **TCP**, **Redis** (pub/sub), **RabbitMQ** (AMQP), and
**Kafka**.

The design mirrors NestJS: you write *controllers* whose methods carry
`#[MessagePattern]` (request/response) or `#[EventPattern]` (fire-and-forget)
attributes, then run a consumer that listens on a transport. Callers use a
`ClientProxy` with `send()` / `emit()`. The transport only moves bytes — your
handlers never change when you switch protocols.

## Architecture

The package is a **framework-agnostic core** with a thin BowPHP adapter:

```
Contracts/         ServerTransport, ClientTransport, Serializer  (the seams)
Message/           Packet, ResponsePacket, JsonSerializer        (wire format)
Server/            MicroserviceServer, HandlerRegistry,
                   MessagePattern, EventPattern, MicroserviceFactory
Client/            ClientProxy, ClientFactory
Transport/         {Tcp,Redis,RabbitMq,Kafka}{Server,Client}Transport
Bow/               MicroserviceServiceProvider  (the ONLY Bow-coupled file)
```

Every transport implements the same two contracts, so adding a fifth (NATS,
MQTT, gRPC) means writing one server + one client class — nothing else moves.

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

## Install

```bash
composer require bowphp/microservice
```

Then install the extension/library for the transport(s) you use:

- TCP → `ext-sockets` (built in on most PHP)
- Redis → `ext-redis` (phpredis)
- RabbitMQ → `composer require php-amqplib/php-amqplib`
- Kafka → `ext-rdkafka`

## Define handlers

```php
use Bow\Microservice\Server\MessagePattern;
use Bow\Microservice\Server\EventPattern;
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

## Run the consumer (`microservice.php`)

This is the `NestFactory.createMicroservice(...).listen()` equivalent.

```bash
php microservice.php --transport=redis    --patterns=user.find,user.created
php microservice.php --transport=tcp       --host=0.0.0.0 --port=3000
php microservice.php --transport=rabbitmq  --queue=bow_microservice
php microservice.php --transport=kafka     --topics=user_events --group=users
```

Supervise with systemd/supervisord; run N copies for concurrency.

## Call from another service

```php
use Bow\Microservice\Client\ClientFactory;

$proxy = ClientFactory::create('redis', ['host' => '127.0.0.1']);
$proxy->connect();

$user = $proxy->send('user.find', ['id' => 42]); // RPC, blocks for reply
$proxy->emit('user.created', ['id' => 99]);       // fire-and-forget
```

## BowPHP integration

Register the provider so a connected `ClientProxy` is available in the
container:

```php
// config/services.php (or wherever Bow loads providers)
return [
    \Bow\Microservice\Bow\MicroserviceServiceProvider::class,
];
```

Copy `examples/config.microservice.php` to `config/microservice.php` and set
your transport. The provider binds both `ClientProxy::class` and the
`microservice.client` alias. For the consumer's controller instantiation, point
the resolver in `microservice.php` at Bow's container `make()`.

## Notes & limits

- The TCP server handles one connection at a time per process — run multiple
  workers behind a balancer, or swap in an event-loop driver later (the framing
  is unchanged).
- Kafka has no native RPC; the reply-topic approach matches NestJS. For pure
  event streaming, use `emit()` only.
- The default serializer is JSON. Implement `Serializer` for msgpack/protobuf.
- Handler exceptions become an error `ResponsePacket`; the client re-throws them
  as `RpcException`. Events swallow-and-log errors (no caller to notify).
```
