<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Redis pub/sub producer (phpredis).
 *
 * For RPC: publish on "<pattern>", then block-subscribe on "<pattern>.reply"
 * until a message whose id matches arrives, or the timeout elapses. For events:
 * publish and return.
 */
final class RedisClientTransport implements ClientTransport
{
    private ?\Redis $pub = null;

    public function __construct(
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 6379,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly string $replySuffix = '.reply',
        private readonly ?string $password = null,
    ) {
        if (!\extension_loaded('redis')) {
            throw new TransportException('The "redis" (phpredis) extension is required for RedisClientTransport.');
        }
    }

    public function connect(): void
    {
        if ($this->pub instanceof \Redis) {
            return;
        }
        $this->pub = $this->makeConnection(readTimeout: 0.0);
    }

    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket
    {
        $this->connect();

        $replyChannel = $packet->pattern . $this->replySuffix;

        // Dedicated subscriber connection with a bounded read timeout so the
        // blocking subscribe loop can give up after $timeout seconds.
        $sub = $this->makeConnection(readTimeout: $timeout);

        $captured = null;
        $deadline = microtime(true) + $timeout;

        // psubscribe/subscribe blocks; we unsubscribe from inside the callback
        // once we see our correlation id (or let the read timeout fire).
        try {
            $this->pub->publish($packet->pattern, $this->serializer->encode($packet->toArray()));

            $sub->subscribe([$replyChannel], function (\Redis $redis, string $channel, string $message) use (&$captured, $packet, $deadline): void {
                try {
                    $resp = ResponsePacket::fromArray($this->serializer->decode($message));
                } catch (\Throwable) {
                    return;
                }
                if ($resp->id === $packet->id) {
                    $captured = $resp;
                    $redis->unsubscribe([$channel]); // breaks the loop
                    return;
                }
                if (microtime(true) >= $deadline) {
                    $redis->unsubscribe([$channel]);
                }
            });
        } catch (\RedisException) {
            // Read timeout manifests as a RedisException — treated as a timeout below.
        } finally {
            try {
                $sub->close();
            } catch (\Throwable) {
            }
        }

        if ($captured === null) {
            throw new TransportException("Redis RPC for '{$packet->pattern}' timed out after {$timeout}s.");
        }

        return $captured;
    }

    public function emit(Packet $packet): void
    {
        $this->connect();
        $this->pub->publish($packet->pattern, $this->serializer->encode($packet->toArray()));
    }

    public function close(): void
    {
        try {
            $this->pub?->close();
        } catch (\Throwable) {
        }
        $this->pub = null;
    }

    public function name(): string
    {
        return 'redis';
    }

    private function makeConnection(float $readTimeout): \Redis
    {
        $redis = new \Redis();
        if (!@$redis->connect($this->host, $this->port)) {
            throw new TransportException("Redis connect to {$this->host}:{$this->port} failed.");
        }
        if ($this->password !== null && !$redis->auth($this->password)) {
            throw new TransportException('Redis AUTH failed.');
        }
        if ($readTimeout > 0.0) {
            $redis->setOption(\Redis::OPT_READ_TIMEOUT, (string) $readTimeout);
        }

        return $redis;
    }
}
