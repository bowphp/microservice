<?php

declare(strict_types=1);

namespace Bow\Microservice\Message;

use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Exception\SerializationException;

final class JsonSerializer implements Serializer
{
    public function encode(array $payload): string
    {
        try {
            return json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        } catch (\JsonException $e) {
            throw new SerializationException('Failed to encode packet: ' . $e->getMessage(), 0, $e);
        }
    }

    public function decode(string $raw): array
    {
        try {
            $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new SerializationException('Failed to decode packet: ' . $e->getMessage(), 0, $e);
        }

        if (!is_array($decoded)) {
            throw new SerializationException('Decoded packet is not an object.');
        }

        return $decoded;
    }
}
