<?php

declare(strict_types=1);

namespace Bow\Microservice\Message;

/**
 * The on-the-wire envelope shared by every transport.
 *
 * Mirrors NestJS's packet shape: a request carries { pattern, data, id },
 * and a response carries { id, response, isDisposed, err }. Keeping a single
 * envelope is what lets the same handler run unchanged across TCP, Redis,
 * RabbitMQ and Kafka — the transport only moves bytes, never interprets them.
 */
final class Packet
{
    public const KIND_MESSAGE = 'message'; // request/response (RPC)
    public const KIND_EVENT   = 'event';   // fire-and-forget
    public const KIND_RESPONSE = 'response';

    /**
     * @param string                $pattern e.g. "user.created"
     * @param mixed                 $data    JSON-serialisable payload
     * @param string                $id      correlation id (empty for events)
     * @param string                $kind    one of the KIND_* constants
     */
    public function __construct(
        public readonly string $pattern,
        public readonly mixed $data = null,
        public readonly string $id = '',
        public readonly string $kind = self::KIND_MESSAGE,
    ) {
    }

    public static function message(string $pattern, mixed $data, ?string $id = null): self
    {
        return new self($pattern, $data, $id ?? self::newId(), self::KIND_MESSAGE);
    }

    public static function event(string $pattern, mixed $data): self
    {
        return new self($pattern, $data, '', self::KIND_EVENT);
    }

    public function isEvent(): bool
    {
        return $this->kind === self::KIND_EVENT;
    }

    public function toArray(): array
    {
        return [
            'pattern' => $this->pattern,
            'data'    => $this->data,
            'id'      => $this->id,
            'kind'    => $this->kind,
        ];
    }

    public static function fromArray(array $a): self
    {
        return new self(
            (string) ($a['pattern'] ?? ''),
            $a['data'] ?? null,
            (string) ($a['id'] ?? ''),
            (string) ($a['kind'] ?? self::KIND_MESSAGE),
        );
    }

    public static function newId(): string
    {
        return bin2hex(random_bytes(16));
    }
}
