<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Contracts\SubscribableServerTransport;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Redis consumer (phpredis), with two delivery modes:
 *
 *   - RPC patterns (#[MessagePattern]) are pulled from a Redis list via
 *     BRPOP. Multiple consumer processes BRPOP'ing the same list form a
 *     worker pool: Redis delivers each message to exactly one of them.
 *   - Event patterns (#[EventPattern]) are received via SUBSCRIBE. Pub/sub
 *     fans every event out to every subscribed process.
 *
 * Wire shape (must match RedisClientTransport):
 *
 *   - RPC request:  list   "<rpcKeyPrefix><pattern>"   — JSON-encoded Packet
 *   - RPC reply:    list   "<replyKeyPrefix><id>"      — JSON-encoded ResponsePacket
 *                                                        (RPUSH'd + EXPIRE)
 *   - Event:        channel "<pattern>"                — JSON-encoded Packet
 *
 * Consumers with BOTH kinds multiplex them by alternating a short
 * subscribe() session (events) with a short brPop() poll (RPC). Events
 * published while BRPOP is blocking may be missed because pub/sub doesn't
 * queue for offline subscribers — for high-volume, reliability-critical
 * event flows, run a dedicated event-only consumer process alongside the
 * RPC consumer (the library picks the right pattern lists for each).
 */
final class RedisServerTransport implements ServerTransport, SubscribableServerTransport
{
    private ?\Redis $sub = null;
    private ?\Redis $pub = null;
    private bool $running = false;

    /** @var list<string> patterns served by #[MessagePattern] — BRPOP'd */
    private array $messagePatterns = [];

    /** @var list<string> patterns served by #[EventPattern] — SUBSCRIBE'd */
    private array $eventPatterns = [];

    /**
     * @param list<string> $messagePatterns initial RPC patterns (optional — can be set later)
     * @param list<string> $eventPatterns   initial event patterns (optional)
     * @param string       $replyKeyPrefix  must match the client's
     * @param string       $rpcKeyPrefix    must match the client's
     * @param int          $replyTtl        seconds before an unclaimed reply key expires
     */
    public function __construct(
        array $messagePatterns = [],
        array $eventPatterns = [],
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 6379,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly string $replyKeyPrefix = 'bow:reply:',
        private readonly string $rpcKeyPrefix = 'bow:rpc:',
        private readonly ?string $password = null,
        private readonly int $replyTtl = 60,
    ) {
        if (!\extension_loaded('redis')) {
            throw new TransportException('The "redis" (phpredis) extension is required for RedisServerTransport.');
        }
        $this->messagePatterns = array_values($messagePatterns);
        $this->eventPatterns = array_values($eventPatterns);
    }

    public function subscribe(array $messagePatterns, array $eventPatterns = []): void
    {
        // Merge + dedupe so explicit constructor patterns and later auto-discovery
        // can coexist without subscribing to the same queue/channel twice.
        if ($messagePatterns !== []) {
            $this->messagePatterns = array_values(array_unique(
                [...$this->messagePatterns, ...array_values($messagePatterns)],
            ));
        }
        if ($eventPatterns !== []) {
            $this->eventPatterns = array_values(array_unique(
                [...$this->eventPatterns, ...array_values($eventPatterns)],
            ));
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

        if ($this->messagePatterns === [] && $this->eventPatterns === []) {
            throw new TransportException(
                'RedisServerTransport has no patterns to handle. '
                . 'Register controllers with #[MessagePattern] and/or #[EventPattern].',
            );
        }

        $this->running = true;

        if ($this->eventPatterns === []) {
            $this->listenRpc($onPacket);
            return;
        }

        if ($this->messagePatterns === []) {
            $this->listenEvents($onPacket);
            return;
        }

        $this->listenMixed($onPacket);
    }

    /**
     * Multiplex BRPOP (RPC) and SUBSCRIBE (events) in one process.
     *
     * phpredis exposes no non-blocking read primitive, so we alternate:
     *
     *   1. Enter subscribe() with a short OPT_READ_TIMEOUT on the sub
     *      connection. The callback handles events until 1s of silence,
     *      at which point subscribe() throws RedisException and exits.
     *   2. Do a brPop() with a 1s timeout on the RPC queues. Process the
     *      message if one arrives.
     *   3. Loop.
     *
     * Trade-off: while BRPOP is blocking (worst case ~1s), no event
     * subscription is active, so pub/sub events published during that
     * window are missed (Redis pub/sub doesn't queue for offline subs).
     * For high-volume or reliability-critical events, deploy a separate
     * event-only consumer alongside.
     *
     * @param callable(Packet):?ResponsePacket $onPacket
     */
    private function listenMixed(callable $onPacket): void
    {
        $queues = array_map(
            fn (string $p): string => $this->rpcKeyPrefix . $p,
            $this->messagePatterns,
        );

        // 1s read timeout so subscribe() breaks out of its blocking read
        // after a second of no events, yielding to the BRPOP slot.
        $this->sub->setOption(\Redis::OPT_READ_TIMEOUT, '1');

        while ($this->running) {
            try {
                $this->sub->subscribe($this->eventPatterns, function (\Redis $redis, string $channel, string $message) use ($onPacket): void {
                    if (!$this->running) {
                        return;
                    }
                    try {
                        $packet = Packet::fromArray($this->serializer->decode($message));
                    } catch (\Throwable) {
                        return;
                    }
                    // Events never produce a reply — discard whatever onPacket returns.
                    $onPacket($packet);
                });
            } catch (\RedisException) {
                // 1s of no events — yield to RPC.
            }

            if (!$this->running) {
                break;
            }

            $this->processOneRpc($queues, $onPacket);
        }
    }

    /**
     * Pop and handle a single RPC request with a 1s timeout. Used by both
     * the RPC-only listener and the multiplexed loop.
     *
     * @param list<string>                       $queues
     * @param callable(Packet):?ResponsePacket   $onPacket
     */
    private function processOneRpc(array $queues, callable $onPacket): void
    {
        try {
            /** @var array<int,string>|false $result */
            $result = $this->pub->brPop($queues, 1);
        } catch (\RedisException) {
            return;
        }

        if (!is_array($result) || $result === []) {
            return;
        }

        [, $message] = $result;

        try {
            $packet = Packet::fromArray($this->serializer->decode($message));
        } catch (\Throwable) {
            return;
        }

        $response = $onPacket($packet);

        if ($response !== null) {
            $key = $this->replyKeyPrefix . $packet->id;
            $this->pub->rPush($key, $this->serializer->encode($response->toArray()));
            $this->pub->expire($key, $this->replyTtl);
        }
    }

    /**
     * BRPOP loop. Pulls one request at a time from any of the RPC queues
     * via the shared processOneRpc() helper. The 1-second BRPOP timeout
     * makes close() / SIGTERM observable without abandoning in-flight
     * requests.
     *
     * @param callable(Packet):?ResponsePacket $onPacket
     */
    private function listenRpc(callable $onPacket): void
    {
        $queues = array_map(
            fn (string $p): string => $this->rpcKeyPrefix . $p,
            $this->messagePatterns,
        );

        while ($this->running) {
            $this->processOneRpc($queues, $onPacket);
        }
    }

    /**
     * Pub/sub SUBSCRIBE loop for fan-out events. No reply is ever sent.
     *
     * @param callable(Packet):?ResponsePacket $onPacket
     */
    private function listenEvents(callable $onPacket): void
    {
        $this->sub->subscribe($this->eventPatterns, function (\Redis $redis, string $channel, string $message) use ($onPacket): void {
            if (!$this->running) {
                return;
            }

            try {
                $packet = Packet::fromArray($this->serializer->decode($message));
            } catch (\Throwable) {
                return; // ignore malformed input
            }

            // Event handlers never produce a reply — discard whatever onPacket returns.
            $onPacket($packet);
        });
    }

    public function close(): void
    {
        $this->running = false;
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
        // BRPOP / SUBSCRIBE block on the server side with their own timeouts;
        // -1 here means "don't impose an additional client-side read timeout".
        $redis->setOption(\Redis::OPT_READ_TIMEOUT, '-1');

        return $redis;
    }
}
