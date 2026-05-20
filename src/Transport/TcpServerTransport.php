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
    private mixed $server = null; // \Socket|null
    private bool $running = false;

    public function __construct(
        private readonly string $host = '0.0.0.0',
        private readonly int $port = 3000,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly int $backlog = 128,
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
