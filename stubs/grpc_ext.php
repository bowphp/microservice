<?php

declare(strict_types=1);

/**
 * IDE stubs for ext-grpc classes that the grpc/grpc Composer package does NOT
 * ship as PHP source — they live in the C extension only. Defining them here
 * lets intelephense / phpstan resolve the type names referenced from
 * GrpcClientTransport without false-positive "undefined type" errors.
 *
 * Only declared when the extension itself is not loaded. When ext-grpc IS
 * loaded, the real classes are already available and this file is a no-op,
 * so the stubs never shadow the real implementation.
 *
 * Never reached at runtime via these stubs either: GrpcClientTransport
 * checks extension_loaded('grpc') before any code path uses them, and
 * throws a clear TransportException if the extension is missing. The stubs
 * exist purely so static analysis can complete.
 */

namespace Grpc;

if (\extension_loaded('grpc')) {
    return;
}

if (!\class_exists(ChannelCredentials::class, false)) {
    final class ChannelCredentials
    {
        public static function createInsecure(): self
        {
            return new self();
        }

        public static function createSsl(
            ?string $pemRootCerts = null,
            ?string $pemPrivateKey = null,
            ?string $pemCertChain = null,
        ): self {
            return new self();
        }
    }
}

if (!\class_exists(Channel::class, false)) {
    final class Channel
    {
        /** @param array<string,mixed> $args */
        public function __construct(string $target, array $args = [])
        {
        }

        public function close(): void
        {
        }
    }
}

if (!\class_exists(Timeval::class, false)) {
    final class Timeval
    {
        public static function infFuture(): self
        {
            return new self();
        }
    }
}
