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
 * Redis transport producer (phpredis).
 *
 * Wire protocol:
 *
 *   - RPC request   (send):  LPUSH list "<rpcKeyPrefix><pattern>"  → server BRPOPs it,
 *                            runs handler, RPUSHes the reply on
 *                            "<replyKeyPrefix><id>" with TTL.
 *   - RPC reply     (send):  BLPOP "<replyKeyPrefix><id>" with timeout.
 *   - Event         (emit):  PUBLISH on channel "<pattern>" → every subscribed
 *                            consumer process receives a copy (fan-out).
 *
 * The list-based request path is a worker pool: multiple consumer processes
 * can BRPOP the same key and Redis hands each message to exactly one of
 * them — no wasted work, no duplicate handler invocations. Replies use a
 * per-request list (keyed by packet id) so concurrent calls don't interfere
 * and the publish-before-subscribe race that plain pub/sub had is gone:
 * lists queue items regardless of who's reading.
 *
 * Events deliberately keep pub/sub semantics because broadcast (every
 * consumer runs the handler — N audit writers, N email senders…) is the
 * point of an event.
 */
final class RedisClientTransport implements ClientTransport
{
    private ?\Redis $pub = null;

    public function __construct(
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 6379,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly string $replyKeyPrefix = 'bow:reply:',
        private readonly string $rpcKeyPrefix = 'bow:rpc:',
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
        // Default read timeout is fine here — we adjust it per-send around BLPOP.
        $this->pub = $this->makeConnection(readTimeout: 0.0);
    }

    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket
    {
        $this->connect();

        $rpcKey = $this->rpcKeyPrefix . $packet->pattern;
        $replyKey = $this->replyKeyPrefix . $packet->id;

        // LPUSH enqueues the request. Multiple consumer processes BRPOP'ing
        // the same key will be served one-at-a-time by Redis (worker pool).
        // Unlike PUBLISH there's no immediate "no subscriber" signal — if
        // nobody is draining the queue the request just sits there until
        // we hit the BLPOP timeout below.
        $this->pub->lPush($rpcKey, $this->serializer->encode($packet->toArray()));

        // BLPOP needs the connection's read timeout to be > the BLPOP timeout,
        // otherwise the socket read times out before Redis can return. Set it
        // here and restore afterwards so subsequent publish()/etc. don't
        // inherit the long-or-zero timeout. `-1` here means "no client-side
        // read timeout" — the canonical phpredis value to disable timeouts.
        $this->pub->setOption(\Redis::OPT_READ_TIMEOUT, (string) ($timeout + 1.0));

        try {
            // phpredis returns [key, value] on success, [] on timeout.
            /** @var array<int,string>|false $result */
            $result = $this->pub->blPop([$replyKey], (int) ceil($timeout));
        } catch (\RedisException) {
            $result = [];
        } finally {
            $this->pub->setOption(\Redis::OPT_READ_TIMEOUT, '-1');
        }

        if (!is_array($result) || $result === []) {
            throw new TransportException(sprintf(
                "Redis RPC for '%s' timed out after %.1fs. "
                . "Make sure a consumer is running with #[MessagePattern('%s')] "
                . "(it should BRPOP the queue '%s').",
                $packet->pattern,
                $timeout,
                $packet->pattern,
                $rpcKey,
            ));
        }

        return ResponsePacket::fromArray($this->serializer->decode($result[1]));
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
        } catch (\Throwable $e) {
            error_log($e->getMessage());
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
