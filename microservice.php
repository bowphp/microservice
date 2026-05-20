<?php

declare(strict_types=1);

/*
|--------------------------------------------------------------------------
| microservice.php — Multi-protocol consumer entrypoint
|--------------------------------------------------------------------------
|
| This is the BowPHP analogue of `NestFactory.createMicroservice(App, opts)`
| followed by `app.listen()`. It boots a long-running worker that consumes
| messages over the transport you pick and dispatches them to your
| controllers' #[MessagePattern] / #[EventPattern] handlers.
|
| Run it:
|   php microservice.php --transport=redis    --patterns=user.created,user.updated
|   php microservice.php --transport=tcp       --host=0.0.0.0 --port=3000
|   php microservice.php --transport=rabbitmq  --queue=bow_microservice
|   php microservice.php --transport=kafka     --topics=user_events --group=users
|
| Supervise it with systemd / supervisord and run N copies for concurrency.
|
*/

use Bow\Microservice\Server\MicroserviceFactory;

require __DIR__ . '/vendor/autoload.php';

// ---------------------------------------------------------------------------
// 1. Parse CLI options (transport-specific knobs all optional w/ sane defaults)
// ---------------------------------------------------------------------------
$opts = getopt('', [
    'transport:', 'host:', 'port:', 'patterns:', 'queue:',
    'topics:', 'group:', 'brokers:', 'password:',
]);

$transport = $opts['transport'] ?? getenv('MICROSERVICE_TRANSPORT') ?: 'redis';

$config = match ($transport) {
    'tcp' => [
        'host' => $opts['host'] ?? '0.0.0.0',
        'port' => (int) ($opts['port'] ?? 3000),
    ],
    'redis' => [
        'host'     => $opts['host'] ?? '127.0.0.1',
        'port'     => (int) ($opts['port'] ?? 6379),
        'patterns' => array_filter(explode(',', $opts['patterns'] ?? '')),
        'password' => $opts['password'] ?? null,
    ],
    'rabbitmq' => [
        'host'  => $opts['host'] ?? '127.0.0.1',
        'port'  => (int) ($opts['port'] ?? 5672),
        'queue' => $opts['queue'] ?? 'bow_microservice',
    ],
    'kafka' => [
        'topics'   => array_filter(explode(',', $opts['topics'] ?? '')),
        'group_id' => $opts['group'] ?? 'bow-microservice',
        'brokers'  => $opts['brokers'] ?? '127.0.0.1:9092',
    ],
    default => throw new RuntimeException("Unknown transport: {$transport}"),
};

// ---------------------------------------------------------------------------
// 2. A tiny stderr logger (swap for BowPHP's logger / Monolog in real apps).
// ---------------------------------------------------------------------------
$logger = new class extends \Psr\Log\AbstractLogger {
    public function log($level, string|\Stringable $message, array $context = []): void
    {
        fwrite(STDERR, sprintf('[%s] %s %s%s', date('H:i:s'), strtoupper((string) $level), $message, PHP_EOL));
    }
};

// ---------------------------------------------------------------------------
// 3. Resolver: how controller classes get instantiated. Plug your DI container
//    here. With BowPHP you might delegate to its container's `make()`.
// ---------------------------------------------------------------------------
$resolver = static function (string $class): object {
    // Example Bow integration:
    //   return \Bow\Container\Capsule::getInstance()->make($class);
    return new $class();
};

// ---------------------------------------------------------------------------
// 4. Build the server and register your controllers.
// ---------------------------------------------------------------------------
$server = MicroserviceFactory::create($transport, $config, $resolver, $logger);

$server->registerControllers(
    // \App\Microservice\UserController::class,
    \Examples\UserController::class,
);

// ---------------------------------------------------------------------------
// 5. Graceful shutdown on SIGTERM/SIGINT (so supervisors can stop cleanly).
// ---------------------------------------------------------------------------
if (function_exists('pcntl_signal') && function_exists('pcntl_async_signals')) {
    pcntl_async_signals(true);
    $stop = static function () use ($server, $logger): void {
        $logger->info('shutting down...');
        $server->stop();
        exit(0);
    };
    pcntl_signal(SIGTERM, $stop);
    pcntl_signal(SIGINT, $stop);
}

// ---------------------------------------------------------------------------
// 6. Block and consume. This is the equivalent of NestApp.listen().
// ---------------------------------------------------------------------------
$server->listen();
