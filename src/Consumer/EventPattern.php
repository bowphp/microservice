<?php

declare(strict_types=1);

namespace Bow\Microservice\Consumer;

/**
 * Marks a method as a fire-and-forget event handler for a given pattern.
 * Equivalent to Nest's @EventPattern(). No value is returned to a caller.
 */
#[\Attribute(\Attribute::TARGET_METHOD)]
final class EventPattern
{
    public function __construct(
        public readonly string $pattern,
    ) {
    }
}
