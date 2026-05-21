<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport\Grpc;

use Bow\Microservice\Exception\SerializationException;

/**
 * Tiny protobuf encoder/decoder for a single bytes field (tag 1, wire type 2).
 *
 * We don't use the google/protobuf runtime here because the envelope is
 * intentionally trivial — one field — and pulling in the descriptor machinery
 * just to wrap a JSON blob would add a hard dependency for no value. The
 * generated PHP from `protoc microservice.proto` would do exactly the same
 * bytes for this shape.
 *
 * Wire format reference: https://protobuf.dev/programming-guides/encoding/
 */
final class ProtobufCodec
{
    /**
     * Wrap raw bytes as `bytes payload = 1` and return the protobuf encoding.
     */
    public static function encodeBytesField(string $payload, int $fieldNumber = 1): string
    {
        // tag = (field_number << 3) | wire_type ; wire_type 2 = length-delimited
        $tag = ($fieldNumber << 3) | 2;

        return chr($tag) . self::encodeVarint(strlen($payload)) . $payload;
    }

    /**
     * Reverse of encodeBytesField. Returns the inner bytes.
     *
     * Tolerates an empty input by returning '' — the Emit RPC's response is an
     * empty MessageEnvelope (no payload field), which decodes to "".
     */
    public static function decodeBytesField(string $bytes, int $expectedFieldNumber = 1): string
    {
        if ($bytes === '') {
            return '';
        }

        $offset = 0;
        $tag = ord($bytes[$offset++]);
        $fieldNumber = $tag >> 3;
        $wireType = $tag & 0x07;

        if ($fieldNumber !== $expectedFieldNumber || $wireType !== 2) {
            throw new SerializationException(sprintf(
                'Expected protobuf field %d (length-delimited), got field %d wire-type %d',
                $expectedFieldNumber,
                $fieldNumber,
                $wireType,
            ));
        }

        $length = self::decodeVarint($bytes, $offset);

        if ($length < 0 || $offset + $length > strlen($bytes)) {
            throw new SerializationException('Protobuf payload length exceeds available bytes');
        }

        return substr($bytes, $offset, $length);
    }

    private static function encodeVarint(int $n): string
    {
        if ($n < 0) {
            throw new SerializationException('Negative lengths are not supported');
        }

        $out = '';
        while ($n >= 0x80) {
            $out .= chr(($n & 0x7F) | 0x80);
            $n >>= 7;
        }
        $out .= chr($n);

        return $out;
    }

    private static function decodeVarint(string $bytes, int &$offset): int
    {
        $result = 0;
        $shift = 0;
        $len = strlen($bytes);

        while ($offset < $len) {
            $b = ord($bytes[$offset++]);
            $result |= ($b & 0x7F) << $shift;
            if (($b & 0x80) === 0) {
                return $result;
            }
            $shift += 7;
            if ($shift >= 64) {
                throw new SerializationException('Varint too long');
            }
        }

        throw new SerializationException('Truncated varint');
    }
}
