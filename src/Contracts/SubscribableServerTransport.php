<?php

declare(strict_types=1);

namespace Bow\Microservice\Contracts;

/**
 * Marker for server transports that need an explicit subscription list
 * (Redis lists/channels, Kafka topics, etc.) before they start listening.
 *
 * MicroserviceServer::listen() detects this interface and pushes the two
 * pattern kinds from the HandlerRegistry. Differentiating them matters
 * because RPC and events have opposite delivery semantics:
 *
 *   - RPC      (#[MessagePattern]) — exactly-one consumer per message
 *     (worker-pool / queue-based delivery).
 *   - Event    (#[EventPattern])    — broadcast: every subscribed consumer
 *     receives a copy.
 *
 * Each transport implements the two semantics using whatever primitive is
 * natural for its protocol (Redis: LIST+BRPOP for RPC, pub/sub for events;
 * Kafka: consumer-group for RPC, separate topic for events; etc.). Transports
 * that don't need this differentiation (e.g. TCP, gRPC) simply don't
 * implement the interface.
 */
interface SubscribableServerTransport
{
    /**
     * Tell the transport which patterns to receive, separated by delivery
     * semantics. Idempotent; safe to call multiple times. Must be called
     * before listen().
     *
     * @param list<string> $messagePatterns RPC patterns — exactly-one delivery
     * @param list<string> $eventPatterns   event patterns — broadcast/fan-out
     */
    public function subscribe(array $messagePatterns, array $eventPatterns = []): void;
}
