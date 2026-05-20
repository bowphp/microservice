<?php

declare(strict_types=1);

namespace Bow\Microservice\Client;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Transport\KafkaClientTransport;
use Bow\Microservice\Transport\RabbitMqClientTransport;
use Bow\Microservice\Transport\RedisClientTransport;
use Bow\Microservice\Transport\TcpClientTransport;

final class ClientFactory
{
    public const TCP = 'tcp';
    public const REDIS = 'redis';
    public const RABBITMQ = 'rabbitmq';
    public const KAFKA = 'kafka';

    /** @param array<string,mixed> $options */
    public static function create(string $transport, array $options = [], float $defaultTimeout = 5.0): ClientProxy
    {
        return new ClientProxy(self::makeTransport($transport, $options), $defaultTimeout);
    }

    /** @param array<string,mixed> $o */
    private static function makeTransport(string $transport, array $o): ClientTransport
    {
        return match ($transport) {
            self::TCP => new TcpClientTransport(
                host: (string) ($o['host'] ?? '127.0.0.1'),
                port: (int) ($o['port'] ?? 3000),
            ),
            self::REDIS => new RedisClientTransport(
                host: (string) ($o['host'] ?? '127.0.0.1'),
                port: (int) ($o['port'] ?? 6379),
                password: $o['password'] ?? null,
            ),
            self::RABBITMQ => new RabbitMqClientTransport(
                queue: (string) ($o['queue'] ?? 'bow_microservice'),
                host: (string) ($o['host'] ?? '127.0.0.1'),
                port: (int) ($o['port'] ?? 5672),
                user: (string) ($o['user'] ?? 'guest'),
                password: (string) ($o['password'] ?? 'guest'),
                vhost: (string) ($o['vhost'] ?? '/'),
            ),
            self::KAFKA => new KafkaClientTransport(
                requestTopic: (string) ($o['topic'] ?? 'bow_microservice'),
                brokers: (string) ($o['brokers'] ?? '127.0.0.1:9092'),
                replyTopic: $o['reply_topic'] ?? null,
            ),
            default => throw new TransportException("Unknown transport '{$transport}'."),
        };
    }
}
