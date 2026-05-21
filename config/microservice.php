<?php

declare(strict_types=1);

/*
| config/microservice.php (BowPHP)
|
| Read by Bow\Microservice\Bow\MicroserviceConfiguration::create() via the
| host application's Loader. The provider:
|
|   1. picks the active transport from `transport`,
|   2. reads its options from the per-transport block (e.g. `redis`, `tcp`),
|   3. builds a ClientProxy through ClientFactory and binds it as
|      ClientProxy::class and 'microservice.client' in the container.
|
| If this file is absent the provider falls back to MICROSERVICE_* env vars
| so the package also works without any host-app config.
*/

return [
    /*
    | The active transport. Only the block matching this value below is read
    | by the provider; the other blocks are ignored and can be left as-is for
    | reference / quick switching.
    |
    | Supported: tcp | redis | rabbitmq | kafka
    */
    'transport' => app_env('MICROSERVICE_TRANSPORT', 'redis'),

    /*
    | Request timeout in seconds for synchronous send() calls. Does not apply
    | to fire-and-forget emit() calls.
    */
    'timeout' => (float) app_env('MICROSERVICE_TIMEOUT', 5.0),

    /*
    | Consumer controllers registered by `microservice.php`. Each entry is
    | a fully qualified class name annotated with #[MessagePattern] and/or
    | #[EventPattern] methods. Override per-process with `--controllers=...`.
    */
    'controllers' => [
        // \App\Consumers\UserConsumer::class,
    ],

    /*
    | TCP transport — talks to a microservice server bound on host:port over
    | a raw socket. Lightweight; no broker required.
    */
    'tcp' => [
        'host' => app_env('MICROSERVICE_HOST', '127.0.0.1'),
        'port' => (int) app_env('MICROSERVICE_PORT', 3000),
    ],

    /*
    | Redis transport — uses Redis as a pub/sub + RPC channel. Requires the
    | phpredis extension on the client side.
    */
    'redis' => [
        'host'     => app_env('MICROSERVICE_HOST', '127.0.0.1'),
        'port'     => (int) app_env('MICROSERVICE_PORT', 6379),
        'password' => app_env('MICROSERVICE_REDIS_PASSWORD', null),

        /*
        | Pub/sub channels the CONSUMER subscribes to. Ignored by the client.
        | Override per-process with `--patterns=foo,bar` or
        | MICROSERVICE_PATTERNS=foo,bar. RedisServerTransport refuses to start
        | with an empty list — Redis pub/sub has no auto-discovery.
        */
        'patterns' => array_values(array_filter(array_map(
            'trim',
            explode(',', (string) app_env('MICROSERVICE_PATTERNS', '')),
        ))),
    ],

    /*
    | RabbitMQ transport — durable queue-backed messaging. Requires the
    | php-amqplib/php-amqplib package.
    */
    'rabbitmq' => [
        'host'     => app_env('MICROSERVICE_HOST', '127.0.0.1'),
        'port'     => (int) app_env('MICROSERVICE_PORT', 5672),
        'user'     => app_env('MICROSERVICE_RABBIT_USER', 'guest'),
        'password' => app_env('MICROSERVICE_RABBIT_PASSWORD', 'guest'),
        'queue'    => app_env('MICROSERVICE_QUEUE', 'bow_microservice'),
    ],

    /*
    | gRPC transport — client only. PHP has no native gRPC server, so
    | implement the server in any language that does (Go/Node/Rust/Java)
    | following proto/microservice.proto. Requires the `grpc` PHP extension
    | (pecl install grpc) and the grpc/grpc Composer package.
    */
    'grpc' => [
        'host' => app_env('MICROSERVICE_HOST', '127.0.0.1'),
        'port' => (int) app_env('MICROSERVICE_PORT', 50051),
    ],

    /*
    | Kafka transport — high-throughput streaming. Requires the rdkafka
    | PHP extension. `brokers` is a comma-separated host:port list.
    */
    'kafka' => [
        'brokers' => app_env('MICROSERVICE_BROKERS', '127.0.0.1:9092'),
        'topic'   => app_env('MICROSERVICE_TOPIC', 'bow_microservice'),
    ],
];
