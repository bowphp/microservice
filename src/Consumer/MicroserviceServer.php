<?php

declare(strict_types=1);

namespace Bow\Microservice\Consumer;

use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Exception\HandlerNotFoundException;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * The consumer. Analogous to a NestJS microservice app: you hand it a
 * transport and a set of controllers, then call listen(). It owns the
 * dispatch loop and knows nothing about wire formats or protocols.
 */
final class MicroserviceServer
{
    private LoggerInterface $logger;

    public function __construct(
        private readonly ServerTransport $transport,
        private readonly HandlerRegistry $registry,
        ?LoggerInterface $logger = null,
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    /** @param class-string ...$controllers */
    public function registerControllers(string ...$controllers): self
    {
        foreach ($controllers as $controller) {
            $this->registry->registerController($controller);
        }

        return $this;
    }

    /**
     * Boot the transport and run the consume loop. Blocks.
     */
    public function listen(): void
    {
        $this->transport->connect();
        $this->logger->info(sprintf(
            '[microservice] listening via "%s" — patterns: %s',
            $this->transport->name(),
            implode(', ', $this->registry->patterns()) ?: '(none)'
        ));

        $this->transport->listen(function (Packet $packet): ?ResponsePacket {
            return $this->handle($packet);
        });
    }

    public function stop(): void
    {
        $this->transport->close();
    }

    /**
     * Run one packet through the registry and shape the reply. Pure logic,
     * unit-testable without any transport.
     */
    public function handle(Packet $packet): ?ResponsePacket
    {
        // Events never produce a reply, even on failure — just log.
        if ($packet->isEvent()) {
            try {
                $this->registry->dispatch($packet);
            } catch (\Throwable $e) {
                $this->logger->error(sprintf(
                    '[microservice] event "%s" failed: %s',
                    $packet->pattern,
                    $e->getMessage()
                ));
            }
            return null;
        }

        try {
            $result = $this->registry->dispatch($packet);
            return ResponsePacket::ok($packet->id, $result);
        } catch (HandlerNotFoundException $e) {
            $this->logger->warning('[microservice] ' . $e->getMessage());
            return ResponsePacket::error($packet->id, $e->getMessage());
        } catch (\Throwable $e) {
            $this->logger->error(sprintf(
                '[microservice] handler for "%s" threw: %s',
                $packet->pattern,
                $e->getMessage()
            ));
            return ResponsePacket::error($packet->id, $e->getMessage());
        }
    }
}
