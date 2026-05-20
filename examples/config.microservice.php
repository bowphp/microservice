<?php

declare(strict_types=1);

/*
| config/microservice.php (BowPHP)
|
| Read by MicroserviceServiceProvider::config(). Pick a transport and the
| options for it. This drives the ClientProxy bound into the container; the
| consumer side is launched via microservice.php.
*/

return [
    // tcp | redis | rabbitmq | kafka
    'transport' => env('MICROSERVICE_TRANSPORT', 'redis'),

    'timeout' => (float) env('MICROSERVICE_TIMEOUT', 5.0),

    'options' => [
        'host'     => env('MICROSERVICE_HOST', '127.0.0.1'),
        'port'     => (int) env('MICROSERVICE_PORT', 6379),

        // redis
        'password' => env('MICROSERVICE_REDIS_PASSWORD', null),

        // rabbitmq
        'queue'    => env('MICROSERVICE_QUEUE', 'bow_microservice'),
        'user'     => env('MICROSERVICE_RABBIT_USER', 'guest'),

        // kafka
        'brokers'  => env('MICROSERVICE_BROKERS', '127.0.0.1:9092'),
        'topic'    => env('MICROSERVICE_TOPIC', 'bow_microservice'),
    ],
];
