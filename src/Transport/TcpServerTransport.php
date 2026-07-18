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
 * Blocking, single-threaded TCP server. Frames are length-prefixed JSON:
 * a 4-byte big-endian unsigned length, then that many bytes of payload.
 *
 * This handles one client connection at a time (accept → serve → loop), which
 * is the simplest correct model and matches how a worker process is typically
 * supervised (run N processes behind a load balancer). For high concurrency
 * you'd swap in an event-loop driver, but the framing stays identical.
 */
final class TcpServerTransport implements ServerTransport
{
    /**
     * Upper bound on an incoming frame's declared length. The 4-byte header can
     * claim up to 4 GiB; without a cap a single crafted header makes the server
     * attempt to buffer that much into memory (a memory-exhaustion DoS). 8 MiB
     * is generous for JSON control messages — raise it via the constructor if
     * your payloads are genuinely larger.
     */
    public const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;

    private mixed $server = null; // \Socket|null
    private bool $running = false;

    /**
     * @param string $host     Bind address. Defaults to loopback; pass '0.0.0.0'
     *                         explicitly to expose the service on all interfaces,
     *                         and only behind network isolation (this transport
     *                         is unauthenticated).
     * @param int    $maxFrameBytes Reject frames whose header exceeds this size.
     * @param float  $readTimeout   Per-read idle timeout (seconds); a client that
     *                         stalls mid-frame is dropped instead of blocking the
     *                         single-threaded accept loop. 0 disables the timeout.
     */
    public function __construct(
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 3000,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly int $backlog = 128,
        private readonly int $maxFrameBytes = self::DEFAULT_MAX_FRAME_BYTES,
        private readonly float $readTimeout = 30.0,
    ) {
        if (!\extension_loaded('sockets')) {
            throw new TransportException('The "sockets" extension is required for TcpServerTransport.');
        }
    }

    public function connect(): void
    {
        $sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($sock === false) {
            throw new TransportException('socket_create failed: ' . $this->lastError());
        }
        socket_set_option($sock, SOL_SOCKET, SO_REUSEADDR, 1);

        if (!@socket_bind($sock, $this->host, $this->port)) {
            throw new TransportException("bind {$this->host}:{$this->port} failed: " . $this->lastError($sock));
        }
        if (!@socket_listen($sock, $this->backlog)) {
            throw new TransportException('listen failed: ' . $this->lastError($sock));
        }

        $this->server = $sock;
    }

    public function listen(callable $onPacket): void
    {
        if ($this->server === null) {
            throw new TransportException('connect() must be called before listen().');
        }

        $this->running = true;

        while ($this->running) {
            $client = @socket_accept($this->server);
            if ($client === false) {
                continue;
            }

            // A stalled client must not hold the single-threaded accept loop:
            // give each blocking read a deadline so an idle peer is dropped.
            if ($this->readTimeout > 0) {
                @socket_set_option($client, SOL_SOCKET, SO_RCVTIMEO, [
                    'sec' => (int) $this->readTimeout,
                    'usec' => (int) (($this->readTimeout - (int) $this->readTimeout) * 1_000_000),
                ]);
            }

            try {
                $this->serveClient($client, $onPacket);
            } catch (\Throwable) {
                // Drop the broken connection, keep the server alive.
            } finally {
                @socket_close($client);
            }
        }
    }

    /** @param callable(Packet):?ResponsePacket $onPacket */
    private function serveClient(mixed $client, callable $onPacket): void
    {
        while ($this->running) {
            $raw = $this->readFrame($client);
            if ($raw === null) {
                return; // peer closed
            }

            $packet = Packet::fromArray($this->serializer->decode($raw));
            $response = $onPacket($packet);

            // Events get no reply; RPC gets a correlated response frame.
            if ($response !== null) {
                $this->writeFrame($client, $this->serializer->encode($response->toArray()));
            }
        }
    }

    private function readFrame(mixed $client): ?string
    {
        $header = $this->readExactly($client, 4);
        if ($header === null) {
            return null;
        }
        /** @var array{1:int} $unpacked */
        $unpacked = unpack('N', $header);
        $length = $unpacked[1];
        if ($length === 0) {
            return '';
        }

        // Refuse an oversized declared length before allocating/reading it, so a
        // crafted header can't drive the server into memory exhaustion. Throwing
        // here drops just this connection; the accept loop keeps running.
        if ($length > $this->maxFrameBytes) {
            throw new TransportException(
                "Incoming frame length {$length} exceeds the {$this->maxFrameBytes}-byte limit."
            );
        }

        return $this->readExactly($client, $length);
    }

    private function readExactly(mixed $client, int $bytes): ?string
    {
        $buffer = '';
        while (\strlen($buffer) < $bytes) {
            $chunk = @socket_read($client, $bytes - \strlen($buffer), PHP_BINARY_READ);
            if ($chunk === false || $chunk === '') {
                return null;
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    private function writeFrame(mixed $client, string $payload): void
    {
        $frame = pack('N', \strlen($payload)) . $payload;
        $total = \strlen($frame);
        $written = 0;
        while ($written < $total) {
            $n = @socket_write($client, substr($frame, $written), $total - $written);
            if ($n === false) {
                throw new TransportException('socket_write failed: ' . $this->lastError($client));
            }
            $written += $n;
        }
    }

    public function close(): void
    {
        $this->running = false;
        if ($this->server !== null) {
            @socket_close($this->server);
            $this->server = null;
        }
    }

    public function name(): string
    {
        return 'tcp';
    }

    private function lastError(mixed $sock = null): string
    {
        $code = $sock !== null ? socket_last_error($sock) : socket_last_error();
        return socket_strerror($code);
    }
}
