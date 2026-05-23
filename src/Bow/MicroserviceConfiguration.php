<?php

declare(strict_types=1);

namespace Bow\Microservice\Bow;

use Bow\Configuration\Configuration;
use Bow\Configuration\Loader;
use Bow\Console\Console;
use Bow\Microservice\Client\ClientFactory;
use Bow\Microservice\Client\ClientProxy;
use Bow\Microservice\Console\GenerateConsumerCommand;
use Bow\Microservice\Console\MicroserviceCommand;
use Bow\Microservice\Console\PublishConfigCommand;

/**
 * BowPHP service provider for the microservice client.
 *
 * This is the only file in the package that is coupled to Bow. The rest of
 * `Bow\Microservice\*` (transports, packets, server, registry…) is framework
 * agnostic so it can be reused outside Bow.
 *
 * Lifecycle (defined by {@see Configuration}):
 *
 *   - create(Loader $config): register bindings on the container.
 *   - run():                  boot the bindings (here: eagerly resolve so
 *                             configuration / transport errors surface at app
 *                             boot rather than mid-request).
 *
 * Registration in the host application:
 *
 *   // app/Kernel.php
 *   public function configurations(): array
 *   {
 *       return [
 *           \Bow\Microservice\Bow\MicroserviceConfiguration::class,
 *       ];
 *   }
 *
 * Bindings exposed:
 *
 *   - ClientProxy::class      — type-hint this in controllers / services.
 *   - 'microservice.client'   — string alias for facade / app() lookups.
 *
 * Configuration source: `config/microservice.php` (read via the injected
 * {@see Loader} as `$config['microservice']`). If that key is missing, the
 * provider falls back to `MICROSERVICE_*` environment variables so the package
 * also works in apps that have not added a config file yet.
 */
final class MicroserviceConfiguration extends Configuration
{
    /**
     * Register the microservice client bindings.
     *
     * Called once by {@see Loader::boot()} before any request handling.
     * The factory is a closure so the underlying ClientProxy is only built
     * when something actually resolves the binding — keeping `create()`
     * cheap and side-effect free.
     *
     * @param Loader $config the host app's configuration loader (ArrayAccess)
     */
    public function create(Loader $config): void
    {
        $settings = $this->resolveSettings($config);

        // Single factory shared by both bindings so resolving either key
        // returns the same proxy instance for the lifetime of the container.
        $factory = static function () use ($settings): ClientProxy {
            $proxy = ClientFactory::create(
                $settings['transport'],
                $settings['options'],
                $settings['timeout'],
            );

            // Connect eagerly so the first send() doesn't pay a handshake
            // latency hit; failures here are caught by run().
            $proxy->connect();

            return $proxy;
        };

        $this->container->bind(ClientProxy::class, $factory);
        $this->container->bind('microservice.client', $factory);

        $this->registerCommands();
    }

    /**
     * Boot the client.
     *
     * Eagerly resolving the binding turns a misconfigured transport into a
     * boot-time exception instead of a runtime surprise on the first RPC
     * call. Mirrors the pattern used by `Bow\Queue\QueueConfiguration::run()`.
     */
    public function run(): void
    {
        $this->container->make('microservice.client');
    }

    /**
     * Resolve the effective settings for the configured transport.
     *
     * Resolution order:
     *
     *   1. The `microservice` key in the host app's config (typically
     *      `config/microservice.php`). The transport-specific options are
     *      read from `$cfg[$transport]` (e.g. `$cfg['redis']`).
     *   2. Environment variables (`MICROSERVICE_*`), used when no config
     *      file is present.
     *
     * Returning a flat shape — `transport`, `options`, `timeout` — lets the
     * factory closure stay simple and lets `ClientFactory` stay agnostic of
     * where the values came from.
     *
     * @param Loader $config
     * @return array{transport:string, options:array<string,mixed>, timeout:float}
     */
    private function resolveSettings(Loader $config): array
    {
        $cfg = $config['microservice'] ?? null;

        if (is_array($cfg) && isset($cfg['transport'])) {
            $transport = (string) $cfg['transport'];

            return [
                'transport' => $transport,
                // Per-transport options live under `$cfg[$transport]`,
                // e.g. `$cfg['redis']`. Default to an empty array so
                // ClientFactory can apply its own transport defaults.
                'options'   => (array) ($cfg[$transport] ?? []),
                'timeout'   => (float) ($cfg['timeout'] ?? 5.0),
            ];
        }

        // No config file present — derive everything from environment.
        $transport = app_env('MICROSERVICE_TRANSPORT') ?: 'redis';

        return [
            'transport' => $transport,
            'options'   => self::optionsFromEnv($transport),
            'timeout'   => (float) (app_env('MICROSERVICE_TIMEOUT') ?: 5.0),
        ];
    }

    /**
     * Build the per-transport options array from environment variables.
     *
     * Used only when the host app has no `config/microservice.php`. Each
     * transport pulls only the variables it actually needs; unrelated env
     * vars are ignored.
     *
     * @return array<string,mixed>
     */
    private static function optionsFromEnv(string $transport): array
    {
        return match ($transport) {
            'tcp' => [
                'host' => app_env('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port' => (int) (app_env('MICROSERVICE_PORT') ?: 3000),
            ],
            'redis' => [
                'host'     => app_env('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port'     => (int) (app_env('MICROSERVICE_PORT') ?: 6379),
                'password' => app_env('MICROSERVICE_REDIS_PASSWORD') ?: null,
            ],
            'rabbitmq' => [
                'host'     => app_env('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port'     => (int) (app_env('MICROSERVICE_PORT') ?: 5672),
                'user'     => app_env('MICROSERVICE_RABBIT_USER') ?: 'guest',
                'password' => app_env('MICROSERVICE_RABBIT_PASSWORD') ?: 'guest',
                'queue'    => app_env('MICROSERVICE_QUEUE') ?: 'bow_microservice',
            ],
            'grpc' => [
                'host' => app_env('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port' => (int) (app_env('MICROSERVICE_PORT') ?: 50051),
            ],
            'kafka' => [
                'brokers' => app_env('MICROSERVICE_BROKERS') ?: '127.0.0.1:9092',
                'topic'   => app_env('MICROSERVICE_TOPIC') ?: 'bow_microservice',
            ],
            default => [],
        };
    }

    /**
     * Register the package's console commands.
     *
     * Kept in a dedicated method (rather than inlined in {@see create()}) so
     * the container-binding logic stays focused on bindings and the command
     * surface stays easy to scan in one place.
     */
    private function registerCommands(): void
    {
        Console::register(
            'microservice:publish-config',
            PublishConfigCommand::class,
            'Copy the default config/microservice.php into the host application',
            "Copies the package's bundled config/microservice.php into the host\n"
                . "application's config directory so it can be customized.\n\n"
                . "Usage:\n"
                . "  php bow microservice:publish-config [--force]\n\n"
                . "Options:\n"
                . "  --force   Overwrite an existing config/microservice.php (off by default\n"
                . "            so customized configs cannot be silently lost).",
        );

        Console::register(
            'add:consumer',
            GenerateConsumerCommand::class,
            'Generate a new microservice consumer class',
            "Creates a new consumer class under app/Consumers/ from the bundled stub.\n\n"
                . "Usage:\n"
                . "  php bow add:consumer <ConsumerName>\n\n"
                . "Arguments:\n"
                . "  ConsumerName   Class name of the consumer to generate (e.g. OrderCreatedConsumer)\n\n"
                . "The file is written to app/Consumers/<ConsumerName>.php using the\n"
                . "namespace configured under namespaces.consumer (defaults to App\\Consumers).\n"
                . "Fails if a file with the same name already exists.",
        );

        Console::register(
            'microservice:listen',
            MicroserviceCommand::class,
            'Run the microservice consumer on the configured transport',
            "Boots a MicroserviceServer for the chosen transport and blocks on listen().\n\n"
                . "  --transport=tcp|redis|rabbitmq|kafka  override config('microservice.transport')\n"
                . "  --controllers=Foo,Bar                 comma-separated FQCN list (overrides config)\n"
                . "  --host, --port, --password            transport connection options\n"
                . "  --patterns=a,b      (redis)           channels to subscribe to\n"
                . "  --queue=name        (rabbitmq)        queue to consume from\n"
                . "  --user, --vhost     (rabbitmq)        broker credentials / vhost\n"
                . "  --topics=a,b        (kafka)           topics to subscribe to\n"
                . "  --brokers, --group  (kafka)           kafka cluster + consumer group",
        );
    }
}
