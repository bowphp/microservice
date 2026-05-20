<?php

declare(strict_types=1);

namespace Bow\Microservice\Client;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Exception\RpcException;
use Bow\Microservice\Message\Packet;

/**
 * Caller-side facade. Mirrors Nest's ClientProxy:
 *   - send()  → request/response (RPC), returns the handler's value
 *   - emit()  → fire-and-forget event
 */
final class ClientProxy
{
    public function __construct(
        private readonly ClientTransport $transport,
        private readonly float $defaultTimeout = 5.0,
    ) {
    }

    public function connect(): void
    {
        $this->transport->connect();
    }

    /**
     * RPC. Throws RpcException if the remote handler errored or timed out.
     */
    public function send(string $pattern, mixed $data, ?float $timeout = null): mixed
    {
        $response = $this->transport->send(
            Packet::message($pattern, $data),
            $timeout ?? $this->defaultTimeout
        );

        if ($response->isError()) {
            throw new RpcException($response->err ?? 'Unknown RPC error');
        }

        return $response->response;
    }

    public function emit(string $pattern, mixed $data): void
    {
        $this->transport->emit(Packet::event($pattern, $data));
    }

    public function close(): void
    {
        $this->transport->close();
    }
}
