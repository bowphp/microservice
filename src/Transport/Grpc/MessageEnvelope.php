<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport\Grpc;

/**
 * Wraps a JSON-serialised Packet/ResponsePacket as the protobuf MessageEnvelope
 * defined in proto/microservice.proto.
 *
 * Implements the two methods that Grpc\BaseStub::_simpleRequest needs:
 *   - serializeToString(): string (for the outbound argument)
 *   - static decode(string $bytes): self (passed as the deserialize callback)
 *
 * No inheritance from Google\Protobuf\Internal\Message — that would require
 * registering a FileDescriptorProto. Since the envelope has only one field,
 * we hand-encode it via ProtobufCodec to avoid that dependency.
 */
final class MessageEnvelope
{
    public function __construct(public string $payload = '')
    {
    }

    public function serializeToString(): string
    {
        return ProtobufCodec::encodeBytesField($this->payload);
    }

    public static function decode(string $bytes): self
    {
        return new self(ProtobufCodec::decodeBytesField($bytes));
    }
}
