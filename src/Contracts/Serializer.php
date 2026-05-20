<?php

declare(strict_types=1);

namespace Bow\Microservice\Contracts;

interface Serializer
{
    public function encode(array $payload): string;

    /** @return array<string,mixed> */
    public function decode(string $raw): array;
}
