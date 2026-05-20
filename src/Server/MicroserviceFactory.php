<?php

declare(strict_types=1);

namespace Bow\Microservice\Server;

use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Transport\KafkaServerTransport;
use Bow\Microservice\Transport\RabbitMqServerTransport;
use Bow\Microservice\Transport\RedisServerTransport;
use Bow\Microservice\Transport\TcpServerTransport;
use Psr\Log\LoggerInterface;

/**
 * Convenience builder mirroring NestFactory.createMicroservice(App, options).
 *
 * Usage:
 *   $server = MicroserviceFactory::create('redis', [
 *       'host' => '127.0.0.1', 'patterns' => ['user.created'],
 *   ], $resolver, $logger);
 *   $server->registerControllers(UserController::class)->listen();
 */
final class MicroserviceFactory
{
    public const TCP = 'tcp';
    public const REDIS = 'redis';
    public const RABBITMQ = 'rabbitmq';
    public const KAFKA = 'kafka';

    /**
     * @param array<string,mixed>           $options
     * @param callable(class-string):object $resolver optional DI resolver
     */
    public static function create(
        string $transport,
        array $options = [],
        ?callable $resolver = null,
        ?LoggerInterface $logger = null,
    ): MicroserviceServer {
        $registry = new HandlerRegistry($resolver);
        $serverTransport = self::makeTransport($transport, $options);

        return new MicroserviceServer($serverTransport, $registry, $logger);
    }

    /** @param array<string,mixed> $o */
    private static function makeTransport(string $transport, array $o): ServerTransport
    {
        return match ($transport) {
            self::TCP => new TcpServerTransport(
                host: (string) ($o['host'] ?? '0.0.0.0'),
                port: (int) ($o['port'] ?? 3000),
            ),
            self::REDIS => new RedisServerTransport(
                patterns: (array) ($o['patterns'] ?? []),
                host: (string) ($o['host'] ?? '127.0.0.1'),
                port: (int) ($o['port'] ?? 6379),
                password: $o['password'] ?? null,
            ),
            self::RABBITMQ => new RabbitMqServerTransport(
                queue: (string) ($o['queue'] ?? 'bow_microservice'),
                host: (string) ($o['host'] ?? '127.0.0.1'),
                port: (int) ($o['port'] ?? 5672),
                user: (string) ($o['user'] ?? 'guest'),
                password: (string) ($o['password'] ?? 'guest'),
                vhost: (string) ($o['vhost'] ?? '/'),
            ),
            self::KAFKA => new KafkaServerTransport(
                topics: (array) ($o['topics'] ?? []),
                groupId: (string) ($o['group_id'] ?? 'bow-microservice'),
                brokers: (string) ($o['brokers'] ?? '127.0.0.1:9092'),
            ),
            default => throw new TransportException("Unknown transport '{$transport}'."),
        };
    }
}
