<?php

declare(strict_types=1);

use Bow\Configuration\Loader;
use Bow\Container\Capsule;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\MicroserviceFactory;
use Psr\Log\AbstractLogger;
use Psr\Log\LoggerInterface;

require __DIR__ . '/../vendor/autoload.php';
require __DIR__ . '/UserConsumer.php';

$opts = getopt('', [
    'transport:', 'host:', 'port:', 'patterns:', 'queue:',
    'topics:', 'group:', 'brokers:', 'password:', 'user:',
    'controllers:', 'kernel:', 'help',
]);

$kernelClass = $opts['kernel'] ?? (app_env('MICROSERVICE_KERNEL') ?: 'App\\Kernel');

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
    ?? (app_env('MICROSERVICE_TRANSPORT') ?: null)
    ?? ($cfg['transport'] ?? 'redis')
);

// ---------------------------------------------------------------------------
// 4. Controller list — resolved BEFORE transport options so Redis patterns
//    can be auto-discovered from the controllers' attributes when the user
//    hasn't supplied --patterns / env / config.
// ---------------------------------------------------------------------------

$controllers = isset($opts['controllers'])
    ? array_values(array_filter(array_map('trim', explode(',', $opts['controllers']))))
    : (array) ($cfg['controllers'] ?? []);

// Smoke-test default: if nothing was configured AND the bundled UserConsumer
// fixture is loaded (it is — required at the top of this file), use it. That
// keeps `php examples/microservice.php --transport=redis` working out of the
// box without forcing the user to pass --controllers=UserConsumer every time.
if ($controllers === [] && class_exists('UserConsumer')) {
    $controllers = ['UserConsumer'];
    fwrite(STDERR, "info: no --controllers supplied; defaulting to the bundled UserConsumer.\n");
}

if ($controllers === []) {
    fwrite(STDERR, "error: no controllers registered — set config('microservice.controllers') or pass --controllers=...\n");
    exit(1);
}

$transportOptions = buildTransportOptions($transport, $opts, $cfg, $controllers);

// Pre-flight: catch missing required options here with an actionable hint
// instead of failing deep inside the transport with a generic error.
preflight($transport, $transportOptions);

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
 * @param array<string,string|false> $opts        CLI options from getopt()
 * @param array<string,mixed>        $cfg         Full microservice config block
 * @param list<class-string>         $controllers Registered controllers, used to
 *                                                auto-discover Redis patterns
 *                                                from #[MessagePattern]/#[EventPattern].
 * @return array<string,mixed>
 */
function buildTransportOptions(string $transport, array $opts, array $cfg, array $controllers = []): array
{
    // Per-transport defaults live under `$cfg[$transport]` in config/microservice.php.
    $base = (array) ($cfg[$transport] ?? []);

    // Small helper: CLI > env > config > default
    $pick = static fn(string $cliKey, ?string $envKey, string $cfgKey, mixed $default)
        => $opts[$cliKey]
            ?? ($envKey !== null ? (app_env($envKey) ?: null) : null)
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
            // CLI > env > config[redis][patterns] > auto-discovery from
            // controllers' #[MessagePattern]/#[EventPattern] attributes.
            // Auto-discovery means a working setup needs only --controllers.
            'patterns' => isset($opts['patterns'])
                ? splitCsv($opts['patterns'])
                : (($csv = app_env('MICROSERVICE_PATTERNS')) !== null && $csv !== ''
                    ? splitCsv((string) $csv)
                    : (count((array) ($base['patterns'] ?? [])) > 0
                        ? array_values((array) $base['patterns'])
                        : discoverPatterns($controllers))),
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
            'topics'   => splitCsv($opts['topics'] ?? (app_env('MICROSERVICE_TOPICS') ?: ($base['topic'] ?? ''))),
            'group_id' => (string) ($opts['group'] ?? app_env('MICROSERVICE_GROUP') ?: ($base['group_id'] ?? 'bow-microservice')),
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
 * Reflect on each controller class to extract every pattern string declared
 * via #[MessagePattern] or #[EventPattern]. Used as the last-resort fallback
 * when nothing's been passed on the CLI, env, or config — Redis pub/sub
 * needs the patterns explicitly, but they're already encoded as attributes
 * on the handlers, so requiring the user to list them twice is friction.
 *
 * @param list<class-string> $controllers
 * @return list<string>
 */
function discoverPatterns(array $controllers): array
{
    $patterns = [];

    foreach ($controllers as $class) {
        if (!class_exists($class)) {
            continue;
        }

        $reflection = new ReflectionClass($class);
        foreach ($reflection->getMethods() as $method) {
            foreach ($method->getAttributes() as $attribute) {
                if ($attribute->getName() !== MessagePattern::class
                    && $attribute->getName() !== EventPattern::class
                ) {
                    continue;
                }
                $patterns[] = $attribute->newInstance()->pattern;
            }
        }
    }

    return array_values(array_unique($patterns));
}

/**
 * Validate that the per-transport options array has everything the server
 * transport actually needs to start. We catch the empty-patterns / empty-topics
 * cases here so the user sees a hint mentioning the CLI flag, env var, and
 * config key — instead of a generic "transport refused to start" exception
 * thrown from inside the transport's constructor.
 *
 * @param array<string,mixed> $options
 */
function preflight(string $transport, array $options): void
{
    $missing = match ($transport) {
        'redis' => empty($options['patterns'])
            ? 'redis needs at least one subscription pattern. '
                . "Pass --patterns=foo,bar, set MICROSERVICE_PATTERNS=foo,bar, "
                . "or add 'patterns' => [...] under config('microservice.redis')."
            : null,
        'kafka' => empty($options['topics'])
            ? 'kafka needs at least one topic. '
                . "Pass --topics=foo,bar, set MICROSERVICE_TOPICS=foo,bar, "
                . "or add 'topics' => [...] under config('microservice.kafka')."
            : null,
        default => null,
    };

    if ($missing !== null) {
        fwrite(STDERR, "error: {$missing}\n");
        exit(1);
    }
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
