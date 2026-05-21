<?php

declare(strict_types=1);

namespace Bow\Microservice\Consumer;

/**
 * Marks a method as a request/response (RPC) handler for a given pattern.
 * Equivalent to Nest's @MessagePattern().
 */
#[\Attribute(\Attribute::TARGET_METHOD)]
final class MessagePattern
{
    public function __construct(
        public readonly string $pattern,
    ) {
    }
}
