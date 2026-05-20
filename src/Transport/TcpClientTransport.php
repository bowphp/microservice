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
 * TCP client using the same 4-byte length-prefixed JSON framing as the server.
 * Uses stream sockets (fsockopen) so we get a read timeout for free.
 */
final class TcpClientTransport implements ClientTransport
{
    /** @var resource|null */
    private $conn = null;

    public function __construct(
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 3000,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly float $connectTimeout = 5.0,
    ) {
    }

    public function connect(): void
    {
        if (\is_resource($this->conn)) {
            return;
        }

        $conn = @fsockopen($this->host, $this->port, $errno, $errstr, $this->connectTimeout);
        if ($conn === false) {
            throw new TransportException("TCP connect to {$this->host}:{$this->port} failed: {$errstr} ({$errno})");
        }

        $this->conn = $conn;
    }

    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket
    {
        $this->connect();
        stream_set_timeout($this->conn, (int) $timeout, (int) (($timeout - (int) $timeout) * 1_000_000));

        $this->writeFrame($this->serializer->encode($packet->toArray()));

        $raw = $this->readFrame();
        if ($raw === null) {
            throw new TransportException('TCP read timed out or connection closed while awaiting reply.');
        }

        return ResponsePacket::fromArray($this->serializer->decode($raw));
    }

    public function emit(Packet $packet): void
    {
        $this->connect();
        $this->writeFrame($this->serializer->encode($packet->toArray()));
        // No reply expected for events.
    }

    private function writeFrame(string $payload): void
    {
        $frame = pack('N', \strlen($payload)) . $payload;
        $total = \strlen($frame);
        $written = 0;
        while ($written < $total) {
            $n = @fwrite($this->conn, substr($frame, $written));
            if ($n === false || $n === 0) {
                throw new TransportException('TCP write failed.');
            }
            $written += $n;
        }
        fflush($this->conn);
    }

    private function readFrame(): ?string
    {
        $header = $this->readExactly(4);
        if ($header === null) {
            return null;
        }
        /** @var array{1:int} $unpacked */
        $unpacked = unpack('N', $header);
        $length = $unpacked[1];

        return $length === 0 ? '' : $this->readExactly($length);
    }

    private function readExactly(int $bytes): ?string
    {
        $buffer = '';
        while (\strlen($buffer) < $bytes) {
            $chunk = @fread($this->conn, $bytes - \strlen($buffer));
            $meta = stream_get_meta_data($this->conn);
            if ($meta['timed_out']) {
                return null;
            }
            if ($chunk === false || ($chunk === '' && feof($this->conn))) {
                return null;
            }
            $buffer .= $chunk;
        }

        return $buffer;
    }

    public function close(): void
    {
        if (\is_resource($this->conn)) {
            fclose($this->conn);
        }
        $this->conn = null;
    }

    public function name(): string
    {
        return 'tcp';
    }
}
