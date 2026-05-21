<?php

declare(strict_types=1);

/*
|--------------------------------------------------------------------------
| microservice.php — Multi-protocol consumer entrypoint
|--------------------------------------------------------------------------
|
| This is BowPHP's analogue of `NestFactory.createMicroservice(App, opts)`
| followed by `app.listen()`. It boots a long-running worker that consumes
| messages over the chosen transport and dispatches them to your
| controllers' #[MessagePattern] / #[EventPattern] handlers.
|
| Quick start:
|   php microservice.php --transport=redis    --patterns=user.created,user.updated
|   php microservice.php --transport=tcp       --host=0.0.0.0 --port=3000
|   php microservice.php --transport=rabbitmq  --queue=bow_microservice
|   php microservice.php --transport=kafka     --topics=user_events --group=users
|   php microservice.php --help
|
| Supervise with systemd / supervisord and run N copies for concurrency.
|
|--------------------------------------------------------------------------
| Configuration sources (each step overrides the previous):
|
|   1. defaults baked into this script
|   2. `config/microservice.php` (read via Bow's Loader after boot)
|   3. environment variables (MICROSERVICE_*)
|   4. CLI flags
|
| Controllers come from `config('microservice.controllers')` by default; pass
| `--controllers=Fully\Qualified\Foo,Fully\Qualified\Bar` to override.
|
|--------------------------------------------------------------------------
| Bow integration:
|
|   - The host app's Kernel class is auto-detected (default `App\Kernel`,
|     override with `--kernel=` or `MICROSERVICE_KERNEL`). If found and it
|     extends `Bow\Configuration\Loader`, it is configured + booted so every
|     provider declared in its configurations() runs. Otherwise a vanilla
|     `Loader` is used so `config/*.php` is still loaded.
|   - Controller instantiation goes through `Capsule::make()`, so consumers
|     can use constructor DI just like HTTP controllers.
|   - The PSR-3 logger bound to `Psr\Log\LoggerInterface` in the container
|     is used if present; otherwise we fall back to a stderr logger.
|
*/

use Bow\Configuration\Loader;
use Bow\Container\Capsule;
use Bow\Microservice\Consumer\MicroserviceFactory;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

require __DIR__ . '/vendor/autoload.php';

// ---------------------------------------------------------------------------
// 1. CLI option parsing
// ---------------------------------------------------------------------------
// All flags are optional; everything has a default or a config-file fallback.
// `--help` prints usage and exits.

$opts = getopt('', [
    'transport:', 'host:', 'port:', 'patterns:', 'queue:',
    'topics:', 'group:', 'brokers:', 'password:', 'user:',
    'controllers:', 'kernel:', 'help',
]);

if (isset($opts['help'])) {
    fwrite(STDOUT, <<<USAGE
        Usage: php microservice.php [options]

        Common:
          --transport=tcp|redis|rabbitmq|kafka   (default: redis)
          --controllers=FQCN1,FQCN2              override config('microservice.controllers')
          --kernel=App\\Kernel                    Bow Kernel class (default: App\\Kernel)
          --help                                  show this message

        Connection (transport-specific):
          --host=...  --port=...  --password=...  --user=...

        Subscription:
          --patterns=a,b,c        (redis pattern channels)
          --queue=name            (rabbitmq queue)
          --topics=t1,t2          (kafka topics)
          --group=name            (kafka consumer group)
          --brokers=h:p,h2:p2     (kafka brokers list)
        USAGE);
    fwrite(STDOUT, PHP_EOL);
    exit(0);
}

// ---------------------------------------------------------------------------
// 2. Boot Bow so we have `config()`, the container, and the host app's
//    providers (including MicroserviceConfiguration) wired up.
// ---------------------------------------------------------------------------
// Strategy: try the host app's Kernel; fall back to vanilla Loader so the
// script remains usable in projects that haven't (yet) declared one.

$kernelClass = $opts['kernel'] ?? (getenv('MICROSERVICE_KERNEL') ?: 'App\\Kernel');

/** @var Loader $kernel */
$kernel = (class_exists($kernelClass) && is_subclass_of($kernelClass, Loader::class))
    ? $kernelClass::configure(__DIR__)
    : Loader::configure(__DIR__);

$kernel->boot();

$container = Capsule::getInstance();

// ---------------------------------------------------------------------------
// 3. Resolve effective settings: config file <- env <- CLI flags
// ---------------------------------------------------------------------------
// `config('microservice')` returns the contents of config/microservice.php
// when present. Missing keys fall back to env vars then to hardcoded defaults.

$cfg = (array) ($kernel['microservice'] ?? []);

$transport = (string) (
    $opts['transport']
    ?? (getenv('MICROSERVICE_TRANSPORT') ?: null)
    ?? ($cfg['transport'] ?? 'redis')
);

$transportOptions = buildTransportOptions($transport, $opts, $cfg);

// ---------------------------------------------------------------------------
// 4. Controller list — config default, CLI override
// ---------------------------------------------------------------------------

$controllers = isset($opts['controllers'])
    ? array_values(array_filter(array_map('trim', explode(',', $opts['controllers']))))
    : (array) ($cfg['controllers'] ?? []);

if ($controllers === []) {
    fwrite(STDERR, "warning: no controllers registered — set config('microservice.controllers') or pass --controllers=...\n");
}

// ---------------------------------------------------------------------------
// 5. Logger — prefer the one bound in the container, fall back to stderr
// ---------------------------------------------------------------------------

$logger = resolveLogger($container);

// ---------------------------------------------------------------------------
// 6. Resolver — instantiate controllers through Bow's container so they get
//    constructor DI (database, repositories, services...).
// ---------------------------------------------------------------------------

$resolver = static fn(string $class): object => $container->make($class);

// ---------------------------------------------------------------------------
// 7. Build and start the server
// ---------------------------------------------------------------------------

$server = MicroserviceFactory::create($transport, $transportOptions, $resolver, $logger);

if ($controllers !== []) {
    $server->registerControllers(...$controllers);
}

// Graceful shutdown on SIGTERM/SIGINT so process supervisors can stop cleanly.
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

$logger->info(sprintf('microservice worker started (transport=%s, controllers=%d)', $transport, count($controllers)));

// Block and consume — equivalent to NestApp.listen().
$server->listen();


// ===========================================================================
// helpers
// ===========================================================================

/**
 * Build the per-transport options array from CLI flags, env vars, and the
 * `microservice.<transport>` config block (in that override order).
 *
 * @param array<string,string|false> $opts CLI options from getopt()
 * @param array<string,mixed>        $cfg  Full microservice config block
 * @return array<string,mixed>
 */
function buildTransportOptions(string $transport, array $opts, array $cfg): array
{
    // Per-transport defaults live under `$cfg[$transport]` in config/microservice.php.
    $base = (array) ($cfg[$transport] ?? []);

    // Small helper: CLI > env > config > default
    $pick = static fn(string $cliKey, ?string $envKey, string $cfgKey, mixed $default)
        => $opts[$cliKey]
            ?? ($envKey !== null ? (getenv($envKey) ?: null) : null)
            ?? ($base[$cfgKey] ?? null)
            ?? $default;

    return match ($transport) {
        'tcp' => [
            'host' => (string) $pick('host', 'MICROSERVICE_HOST', 'host', '0.0.0.0'),
            'port' => (int)    $pick('port', 'MICROSERVICE_PORT', 'port', 3000),
        ],
        'redis' => [
            'host'     => (string) $pick('host', 'MICROSERVICE_HOST', 'host', '127.0.0.1'),
            'port'     => (int)    $pick('port', 'MICROSERVICE_PORT', 'port', 6379),
            'password' =>          $pick('password', 'MICROSERVICE_REDIS_PASSWORD', 'password', null),
            // Subscription patterns are inherently worker-side and live outside
            // the connection config; accept them from CLI or env only.
            'patterns' => splitCsv($opts['patterns'] ?? (getenv('MICROSERVICE_PATTERNS') ?: '')),
        ],
        'rabbitmq' => [
            'host'     => (string) $pick('host',     'MICROSERVICE_HOST',            'host',     '127.0.0.1'),
            'port'     => (int)    $pick('port',     'MICROSERVICE_PORT',            'port',     5672),
            'user'     => (string) $pick('user',     'MICROSERVICE_RABBIT_USER',     'user',     'guest'),
            'password' => (string) $pick('password', 'MICROSERVICE_RABBIT_PASSWORD', 'password', 'guest'),
            'queue'    => (string) $pick('queue',    'MICROSERVICE_QUEUE',           'queue',    'bow_microservice'),
        ],
        'kafka' => [
            'brokers'  => (string) $pick('brokers', 'MICROSERVICE_BROKERS', 'brokers', '127.0.0.1:9092'),
            'topics'   => splitCsv($opts['topics'] ?? (getenv('MICROSERVICE_TOPICS') ?: ($base['topic'] ?? ''))),
            'group_id' => (string) ($opts['group'] ?? getenv('MICROSERVICE_GROUP') ?: ($base['group_id'] ?? 'bow-microservice')),
        ],
        default => throw new RuntimeException("Unknown transport: {$transport}"),
    };
}

/** @return list<string> */
function splitCsv(string $csv): array
{
    return array_values(array_filter(array_map('trim', explode(',', $csv)), 'strlen'));
}

/**
 * Return a PSR-3 logger, preferring one bound in Bow's container so the
 * worker shares formatting/handlers with the rest of the host app.
 */
function resolveLogger(Capsule $container): LoggerInterface
{
    if (isset($container[LoggerInterface::class])) {
        $bound = $container->make(LoggerInterface::class);
        if ($bound instanceof LoggerInterface) {
            return $bound;
        }
    }

    // Tiny stderr fallback so the script remains useful without any logger
    // configuration in the host app.
    return new class extends AbstractLogger {
        public function log($level, string|\Stringable $message, array $context = []): void
        {
            fwrite(STDERR, sprintf(
                '[%s] %s %s%s',
                date('H:i:s'),
                strtoupper((string) $level),
                $message,
                PHP_EOL,
            ));
        }
    };
}
