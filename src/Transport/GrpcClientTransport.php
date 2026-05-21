<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Exception\RpcException;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use Bow\Microservice\Transport\Grpc\MessageEnvelope;
use Bow\Microservice\Transport\Grpc\MessageServiceClient;

/**
 * gRPC client transport.
 *
 * Calls a microservice.MessageService.Send / .Emit endpoint defined by
 * proto/microservice.proto. The Packet is JSON-encoded by the configured
 * serializer and wrapped in a MessageEnvelope, so the unified handler model
 * works the same way as on every other transport. Implement the server side
 * in any language that has a real gRPC server (Go, Node, Rust, Java…).
 *
 * Requires the `grpc` PHP extension. The class throws a clear
 * TransportException if the extension is missing rather than failing later
 * with an opaque class-not-found error.
 *
 * Server-side gRPC for PHP is intentionally out of scope: the `grpc`
 * extension is client-only, and a production-grade PHP gRPC server requires
 * RoadRunner or Swoole, which neither matches the existing
 * single-process consumer model nor belongs in this library.
 */
final class GrpcClientTransport implements ClientTransport
{
    /**
     * The active gRPC stub. Typed as `object` because MessageServiceClient is
     * only declared when the `grpc` extension is loaded — tests can inject a
     * duck-typed double exposing Send/Emit so they don't need the extension.
     */
    private ?object $stub = null;

    /**
     * @param array<string,mixed> $channelOptions Forwarded to Grpc\Channel
     *                                            (e.g. ['credentials' => ...]).
     *                                            Defaults to insecure when omitted.
     * @param object|null         $injectedStub   Any object exposing
     *                                            Send(MessageEnvelope) and
     *                                            Emit(MessageEnvelope) returning
     *                                            a wait()-able call.
     */
    public function __construct(
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 50051,
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly array $channelOptions = [],
        private readonly ?object $injectedStub = null,
    ) {
    }

    public function connect(): void
    {
        if ($this->stub !== null) {
            return;
        }

        if ($this->injectedStub !== null) {
            $this->stub = $this->injectedStub;
            return;
        }

        if (!\extension_loaded('grpc')) {
            throw new TransportException(
                'gRPC client requires the "grpc" PHP extension and the grpc/grpc Composer package. '
                . 'Install: pecl install grpc && composer require grpc/grpc google/protobuf'
            );
        }

        $options = $this->channelOptions;
        if (!isset($options['credentials'])) {
            $options['credentials'] = \Grpc\ChannelCredentials::createInsecure();
        }

        $this->stub = new MessageServiceClient("{$this->host}:{$this->port}", $options);
    }

    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket
    {
        $this->connect();

        $envelope = new MessageEnvelope($this->serializer->encode($packet->toArray()));
        $call = $this->stub->Send($envelope, [], ['timeout' => (int) ($timeout * 1_000_000)]);

        /** @var array{0:MessageEnvelope|null, 1:object} $result */
        $result = $call->wait();
        [$reply, $status] = $result;

        if ($status->code !== 0) {
            throw new RpcException(sprintf(
                'gRPC call failed (code=%d): %s',
                $status->code,
                $status->details ?? '',
            ));
        }

        if ($reply === null || $reply->payload === '') {
            throw new RpcException('gRPC reply contained no payload.');
        }

        return ResponsePacket::fromArray($this->serializer->decode($reply->payload));
    }

    public function emit(Packet $packet): void
    {
        $this->connect();

        $envelope = new MessageEnvelope($this->serializer->encode($packet->toArray()));
        $call = $this->stub->Emit($envelope);

        // We still wait() so transport errors surface immediately. The reply
        // body is ignored — events don't carry meaningful responses.
        [, $status] = $call->wait();

        if ($status->code !== 0) {
            throw new TransportException(sprintf(
                'gRPC emit failed (code=%d): %s',
                $status->code,
                $status->details ?? '',
            ));
        }
    }

    public function close(): void
    {
        if ($this->stub !== null && method_exists($this->stub, 'close')) {
            $this->stub->close();
        }
        $this->stub = null;
    }

    public function name(): string
    {
        return 'grpc';
    }
}
