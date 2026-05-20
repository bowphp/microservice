<?php

declare(strict_types=1);

namespace Bow\Microservice\Message;

/**
 * Reply to a request packet, correlated back to the caller by id.
 */
final class ResponsePacket
{
    public function __construct(
        public readonly string $id,
        public readonly mixed $response = null,
        public readonly bool $isDisposed = true,
        public readonly ?string $err = null,
    ) {
    }

    public static function ok(string $id, mixed $response): self
    {
        return new self($id, $response, true, null);
    }

    public static function error(string $id, string $message): self
    {
        return new self($id, null, true, $message);
    }

    public function isError(): bool
    {
        return $this->err !== null;
    }

    public function toArray(): array
    {
        return [
            'id'         => $this->id,
            'response'   => $this->response,
            'isDisposed' => $this->isDisposed,
            'err'        => $this->err,
            'kind'       => Packet::KIND_RESPONSE,
        ];
    }

    public static function fromArray(array $a): self
    {
        return new self(
            (string) ($a['id'] ?? ''),
            $a['response'] ?? null,
            (bool) ($a['isDisposed'] ?? true),
            isset($a['err']) ? (string) $a['err'] : null,
        );
    }
}
