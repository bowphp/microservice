<?php

declare(strict_types=1);

namespace Bow\Microservice\Contracts;

use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Producer side, used by ClientProxy.
 */
interface ClientTransport
{
    public function connect(): void;

    /**
     * Request/response. Blocks until a correlated reply arrives or $timeout
     * (seconds) elapses.
     */
    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket;

    /** Fire-and-forget. Returns once the broker/peer has accepted the packet. */
    public function emit(Packet $packet): void;

    public function close(): void;

    public function name(): string;
}
