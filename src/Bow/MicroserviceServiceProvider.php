<?php

declare(strict_types=1);

namespace Bow\Microservice\Bow;

use Bow\Microservice\Client\ClientFactory;
use Bow\Microservice\Client\ClientProxy;

/**
 * Thin BowPHP adapter. The package core is framework-agnostic; this provider
 * is the *only* file that touches Bow. It registers a ClientProxy in the
 * container so application code can inject it to talk to other services.
 *
 * BowPHP's provider base class is `Bow\Container\Action` / a ServiceProvider in
 * some versions. Rather than hard-extend a class whose namespace varies between
 * Bow releases, this provider is written to be wired up either way:
 *
 *   // config/services.php (Bow >= 4 style)
 *   return [ \Bow\Microservice\Bow\MicroserviceServiceProvider::class ];
 *
 * and exposes start()/boot() so it works whether Bow calls `process()`,
 * `boot()`, or `register()`.
 *
 * Config is read from a `microservice` config namespace (see config example),
 * falling back to env vars so it works without the Bow config system too.
 */
final class MicroserviceServiceProvider
{
    /** @var callable|null injected container binder, set via setBinder() */
    private $binder = null;

    /**
     * Allow the host app to provide a binding closure:
     *   $provider->setBinder(fn(string $abstract, callable $factory) => $container->bind($abstract, $factory));
     */
    public function setBinder(callable $binder): void
    {
        $this->binder = $binder;
    }

    /** BowPHP entrypoints — different versions call different names. */
    public function process(): void
    {
        $this->register();
    }

    public function boot(): void
    {
        $this->register();
    }

    public function register(): void
    {
        $config = self::config();

        $factory = static function () use ($config): ClientProxy {
            $proxy = ClientFactory::create(
                $config['transport'],
                $config['options'],
                (float) ($config['timeout'] ?? 5.0)
            );
            $proxy->connect();
            return $proxy;
        };

        // Prefer an explicit binder if one was injected.
        if ($this->binder !== null) {
            ($this->binder)(ClientProxy::class, $factory);
            ($this->binder)('microservice.client', $factory);
            return;
        }

        // Otherwise try Bow's container facade if present.
        $this->bindViaBowContainer($factory);
    }

    private function bindViaBowContainer(callable $factory): void
    {
        // Bow exposes a Capsule/Container; we probe common shapes without a hard dependency.
        foreach (['\\Bow\\Container\\Capsule', '\\Bow\\Container\\Container'] as $containerClass) {
            if (class_exists($containerClass) && method_exists($containerClass, 'getInstance')) {
                /** @var object $container */
                $container = $containerClass::getInstance();
                if (method_exists($container, 'bind')) {
                    $container->bind(ClientProxy::class, $factory);
                    $container->bind('microservice.client', $factory);
                    return;
                }
            }
        }
        // If no container is found we silently no-op; the app can still use
        // ClientFactory directly. (Kept non-fatal on purpose.)
    }

    /**
     * @return array{transport:string, options:array<string,mixed>, timeout:float}
     */
    public static function config(): array
    {
        // Try Bow's config() helper first, then env, then defaults.
        if (function_exists('config')) {
            /** @var mixed $cfg */
            $cfg = \config('microservice');
            if (is_array($cfg) && isset($cfg['transport'])) {
                return [
                    'transport' => (string) $cfg['transport'],
                    'options'   => (array) ($cfg['options'] ?? []),
                    'timeout'   => (float) ($cfg['timeout'] ?? 5.0),
                ];
            }
        }

        $transport = getenv('MICROSERVICE_TRANSPORT') ?: 'redis';

        return [
            'transport' => $transport,
            'options'   => self::optionsFromEnv($transport),
            'timeout'   => (float) (getenv('MICROSERVICE_TIMEOUT') ?: 5.0),
        ];
    }

    /** @return array<string,mixed> */
    private static function optionsFromEnv(string $transport): array
    {
        return match ($transport) {
            'tcp' => [
                'host' => getenv('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port' => (int) (getenv('MICROSERVICE_PORT') ?: 3000),
            ],
            'redis' => [
                'host'     => getenv('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port'     => (int) (getenv('MICROSERVICE_PORT') ?: 6379),
                'password' => getenv('MICROSERVICE_REDIS_PASSWORD') ?: null,
            ],
            'rabbitmq' => [
                'host'  => getenv('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port'  => (int) (getenv('MICROSERVICE_PORT') ?: 5672),
                'user'  => getenv('MICROSERVICE_RABBIT_USER') ?: 'guest',
                'password' => getenv('MICROSERVICE_RABBIT_PASSWORD') ?: 'guest',
                'queue' => getenv('MICROSERVICE_QUEUE') ?: 'bow_microservice',
            ],
            'kafka' => [
                'brokers' => getenv('MICROSERVICE_BROKERS') ?: '127.0.0.1:9092',
                'topic'   => getenv('MICROSERVICE_TOPIC') ?: 'bow_microservice',
            ],
            default => [],
        };
    }
}
