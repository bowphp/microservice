<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Redis pub/sub consumer (phpredis).
 *
 * Convention, matching NestJS: a request for pattern P is published on channel
 * "P". The reply is published back on channel "P.reply", and callers correlate
 * by the packet id. We subscribe to one Redis channel per registered pattern.
 *
 * Note: a phpredis connection in subscribe mode is blocked, so we use a
 * separate publisher connection for replies.
 */
final class RedisServerTransport implements ServerTransport
{
    private ?\Redis $sub = null;
    private ?\Redis $pub = null;
    private bool $running = false;

    /** @param list<string> $patterns channels to subscribe to */
    public function __construct(
        private readonly array $patterns,
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 6379,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly string $replySuffix = '.reply',
        private readonly ?string $password = null,
    ) {
        if (!\extension_loaded('redis')) {
            throw new TransportException('The "redis" (phpredis) extension is required for RedisServerTransport.');
        }
        if ($patterns === []) {
            throw new TransportException('RedisServerTransport needs at least one pattern/channel to subscribe to.');
        }
    }

    public function connect(): void
    {
        $this->sub = $this->makeConnection();
        $this->pub = $this->makeConnection();
    }

    public function listen(callable $onPacket): void
    {
        if ($this->sub === null || $this->pub === null) {
            throw new TransportException('connect() must be called before listen().');
        }

        $this->running = true;

        // The callback fires for every message on any subscribed channel.
        $this->sub->subscribe($this->patterns, function (\Redis $redis, string $channel, string $message) use ($onPacket): void {
            if (!$this->running) {
                return;
            }

            try {
                $packet = Packet::fromArray($this->serializer->decode($message));
            } catch (\Throwable) {
                return; // ignore malformed input
            }

            $response = $onPacket($packet);

            if ($response !== null) {
                $this->pub->publish(
                    $channel . $this->replySuffix,
                    $this->serializer->encode($response->toArray())
                );
            }
        });
    }

    public function close(): void
    {
        $this->running = false;
        // Best-effort: closing the underlying socket breaks the blocking subscribe.
        try {
            $this->sub?->close();
        } catch (\Throwable) {
        }
        try {
            $this->pub?->close();
        } catch (\Throwable) {
        }
        $this->sub = null;
        $this->pub = null;
    }

    public function name(): string
    {
        return 'redis';
    }

    private function makeConnection(): \Redis
    {
        $redis = new \Redis();
        if (!@$redis->connect($this->host, $this->port)) {
            throw new TransportException("Redis connect to {$this->host}:{$this->port} failed.");
        }
        if ($this->password !== null && !$redis->auth($this->password)) {
            throw new TransportException('Redis AUTH failed.');
        }
        // Subscriber must never time out while waiting for messages.
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, '-1');

        return $redis;
    }
}
