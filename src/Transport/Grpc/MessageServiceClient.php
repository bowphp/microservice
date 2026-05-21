<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport\Grpc;

/**
 * Hand-written stub for the microservice.MessageService gRPC service.
 *
 * Equivalent to what `protoc --php_out=... --grpc_out=...` would generate for
 * proto/microservice.proto, but tailored to our single-field envelope so we
 * don't have to commit the full descriptor machinery.
 *
 * Declared only when the `grpc` PHP extension is loaded. The grpc/grpc
 * Composer package ships PHP source for Grpc\BaseStub (useful for IDE
 * resolution), but instantiating it without the C extension fails at
 * runtime — so we gate on the extension itself, not class existence.
 * GrpcClientTransport surfaces a descriptive error when the extension
 * is missing.
 */
if (\extension_loaded('grpc')) {
    final class MessageServiceClient extends \Grpc\BaseStub
    {
        /**
         * @param string                $hostname        e.g. "127.0.0.1:50051"
         * @param array<string,mixed>   $opts            Grpc channel options
         * @param \Grpc\Channel|null    $channel
         */
        public function __construct(string $hostname, array $opts = [], ?\Grpc\Channel $channel = null)
        {
            parent::__construct($hostname, $opts, $channel);
        }

        /**
         * RPC: send a request and wait for a reply.
         *
         * @return \Grpc\UnaryCall
         */
        public function Send(
            MessageEnvelope $request,
            array $metadata = [],
            array $options = [],
        ) {
            return $this->_simpleRequest(
                '/microservice.MessageService/Send',
                $request,
                [MessageEnvelope::class, 'decode'],
                $metadata,
                $options,
            );
        }

        /**
         * Fire-and-forget event. Server returns an empty envelope; we still
         * use Send semantics so we can observe transport-level errors.
         *
         * @return \Grpc\UnaryCall
         */
        public function Emit(
            MessageEnvelope $request,
            array $metadata = [],
            array $options = [],
        ) {
            return $this->_simpleRequest(
                '/microservice.MessageService/Emit',
                $request,
                [MessageEnvelope::class, 'decode'],
                $metadata,
                $options,
            );
        }
    }
}
