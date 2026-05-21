<?php

declare(strict_types=1);

namespace Bow\Microservice\Console;

use Bow\Console\AbstractCommand;
use Bow\Console\Color;
use Bow\Microservice\Consumer\MicroserviceFactory;
use Bow\Microservice\Consumer\MicroserviceServer;
use Bow\Microservice\Exception\TransportException;
use Throwable;

/**
 * Console entrypoint for the microservice consumer.
 *
 * Registered by MicroserviceConfiguration as `microservice:listen` and dispatched
 * via Console::addCommand, which calls process(). CLI flags override the values
 * from config/microservice.php; anything not on the CLI falls back to config.
 *
 *   php bow microservice:listen --transport=redis --patterns=user.find,user.created
 *   php bow microservice:listen --transport=tcp   --host=0.0.0.0 --port=3000
 *   php bow microservice:listen --transport=rabbitmq --queue=bow_microservice
 *   php bow microservice:listen --transport=kafka --topics=user_events --group=users
 */
final class MicroserviceCommand extends AbstractCommand
{
    /**
     * Boot a MicroserviceServer with the configured transport and block on listen().
     */
    public function process(): void
    {
        $transport = (string) $this->arg->getParameter(
            '--transport',
            $this->configValue('microservice.transport', 'redis'),
        );

        $controllers = $this->resolveControllers();

        if ($controllers === []) {
            echo Color::red(
                "No controllers configured. Pass --controllers=A,B or set 'microservice.controllers'.\n",
            );
            exit(1);
        }

        $options = $this->resolveOptions($transport);

        if ($error = $this->preflight($transport, $options)) {
            echo Color::red("error: {$error}\n");
            exit(1);
        }

        $server = $this->buildServer($transport, $options);
        $server->registerControllers(...$controllers);

        echo Color::green(sprintf(
            "Microservice listening on %s with %d controller(s)\n",
            $transport,
            count($controllers),
        ));

        // Graceful shutdown on SIGTERM/SIGINT so process supervisors can stop cleanly.
        if (function_exists('pcntl_signal') && function_exists('pcntl_async_signals')) {
            pcntl_async_signals(true);

            $stop = static function () use ($server): void {
                echo Color::green(sprintf('shutting down...'));
                $server->stop();
                exit(0);
            };

            pcntl_signal(SIGTERM, $stop);
            pcntl_signal(SIGINT, $stop);
        }

        $server->listen();
    }

    /**
     * @param array<string,mixed> $options
     */
    private function buildServer(string $transport, array $options): MicroserviceServer
    {
        $resolver = static fn (string $class): object => app($class);

        try {
            return MicroserviceFactory::create($transport, $options, $resolver);
        } catch (TransportException $e) {
            echo Color::red($e->getMessage() . "\n");
            exit(1);
        }
    }

    /**
     * Surface missing-required-option cases here with an actionable hint
     * instead of letting the transport throw a generic "needs at least one
     * pattern/topic" deep in its constructor.
     *
     * @param array<string,mixed> $options
     */
    private function preflight(string $transport, array $options): ?string
    {
        return match ($transport) {
            MicroserviceFactory::REDIS => empty($options['patterns'])
                ? "redis needs at least one subscription pattern. "
                    . "Pass --patterns=foo,bar or set 'patterns' => [...] "
                    . "under config('microservice.redis')."
                : null,
            MicroserviceFactory::KAFKA => empty($options['topics'])
                ? "kafka needs at least one topic. Pass --topics=foo,bar "
                    . "or set 'topics' => [...] under config('microservice.kafka')."
                : null,
            default => null,
        };
    }

    /**
     * @return list<class-string>
     */
    private function resolveControllers(): array
    {
        $fromCli = $this->arg->getParameter('--controllers');

        if (is_string($fromCli) && $fromCli !== '') {
            return array_values(array_filter(array_map('trim', explode(',', $fromCli))));
        }

        return array_values((array) $this->configValue('microservice.controllers', []));
    }

    /**
     * @return array<string,mixed>
     */
    private function resolveOptions(string $transport): array
    {
        $config = (array) $this->configValue("microservice.{$transport}", []);

        return match ($transport) {
            MicroserviceFactory::TCP => [
                'host' => (string) $this->arg->getParameter('--host', $config['host'] ?? '0.0.0.0'),
                'port' => (int) $this->arg->getParameter('--port', $config['port'] ?? 3000),
            ],
            MicroserviceFactory::REDIS => [
                'host'     => (string) $this->arg->getParameter('--host', $config['host'] ?? '127.0.0.1'),
                'port'     => (int) $this->arg->getParameter('--port', $config['port'] ?? 6379),
                'password' => $this->arg->getParameter('--password', $config['password'] ?? null),
                'patterns' => $this->parseList($this->arg->getParameter('--patterns'), (array) ($config['patterns'] ?? [])),
            ],
            MicroserviceFactory::RABBITMQ => [
                'host'     => (string) $this->arg->getParameter('--host', $config['host'] ?? '127.0.0.1'),
                'port'     => (int) $this->arg->getParameter('--port', $config['port'] ?? 5672),
                'user'     => (string) $this->arg->getParameter('--user', $config['user'] ?? 'guest'),
                'password' => (string) $this->arg->getParameter('--password', $config['password'] ?? 'guest'),
                'vhost'    => (string) $this->arg->getParameter('--vhost', $config['vhost'] ?? '/'),
                'queue'    => (string) $this->arg->getParameter('--queue', $config['queue'] ?? 'bow_microservice'),
            ],
            MicroserviceFactory::KAFKA => [
                'brokers'  => (string) $this->arg->getParameter('--brokers', $config['brokers'] ?? '127.0.0.1:9092'),
                'topics'   => $this->parseList($this->arg->getParameter('--topics'), (array) ($config['topics'] ?? [])),
                'group_id' => (string) $this->arg->getParameter('--group', $config['group_id'] ?? 'bow-microservice'),
            ],
            default => [],
        };
    }

    /**
     * Parse a comma-separated CLI value, falling back to the given array.
     *
     * @param mixed                $cliValue
     * @param array<int,string>    $default
     * @return array<int,string>
     */
    private function parseList(mixed $cliValue, array $default): array
    {
        if (is_string($cliValue) && $cliValue !== '') {
            return array_values(array_filter(array_map('trim', explode(',', $cliValue))));
        }

        return $default;
    }

    /**
     * Safe wrapper around config() that returns $default when Bow's config
     * Loader has not been booted (e.g. in unit tests or standalone CLI runs).
     */
    private function configValue(string $key, mixed $default = null): mixed
    {
        try {
            return config($key, $default);
        } catch (Throwable) {
            return $default;
        }
    }
}
