<?php

declare(strict_types=1);

namespace Bow\Microservice\Contracts;

use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Consumer side. Implemented once per protocol (TCP, Redis, AMQP, Kafka).
 *
 * The contract is deliberately tiny: a transport knows how to receive raw
 * inbound packets and, for RPC, how to send a correlated reply back. It does
 * NOT know about handlers, patterns, or business logic — that lives in the
 * MicroserviceServer which is fully transport-agnostic.
 */
interface ServerTransport
{
    public function connect(): void;

    /**
     * Block and pump inbound packets into $onPacket until stopped.
     *
     * The handler returns a ResponsePacket for RPC messages, or null for
     * events. The transport is responsible for routing that reply back to
     * the right caller using whatever mechanism the protocol provides
     * (TCP: same socket; Redis: a reply channel; AMQP: replyTo + correlationId;
     * Kafka: a reply topic).
     *
     * @param callable(Packet):?ResponsePacket $onPacket
     */
    public function listen(callable $onPacket): void;

    public function close(): void;

    /** Stable name for logging/diagnostics, e.g. "redis", "tcp". */
    public function name(): string;
}
