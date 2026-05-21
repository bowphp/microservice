<?php

declare(strict_types=1);

namespace Bow\Microservice\Tests;

use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Consumer\HandlerRegistry;
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\MicroserviceServer;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Contracts\SubscribableServerTransport;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use PHPUnit\Framework\TestCase;

final class AutoSubscribeFixtureController
{
    #[MessagePattern('user.find')]
    public function find(): array
    {
        return [];
    }

    #[MessagePattern('math.sum')]
    public function sum(): int
    {
        return 0;
    }

    #[EventPattern('user.created')]
    public function created(): void
    {
    }
}

/**
 * Records the patterns passed to subscribe() and skips actual listening so
 * the test can return synchronously.
 */
final class RecordingSubscribableTransport implements ServerTransport, SubscribableServerTransport
{
    /** @var list<string> */
    public array $messageSubscriptions = [];

    /** @var list<string> */
    public array $eventSubscriptions = [];

    public function subscribe(array $messagePatterns, array $eventPatterns = []): void
    {
        $this->messageSubscriptions = array_values(array_unique([...$this->messageSubscriptions, ...$messagePatterns]));
        $this->eventSubscriptions = array_values(array_unique([...$this->eventSubscriptions, ...$eventPatterns]));
    }

    public function connect(): void
    {
    }

    public function listen(callable $onPacket): void
    {
        // intentionally non-blocking for the test
    }

    public function close(): void
    {
    }

    public function name(): string
    {
        return 'recording';
    }
}

/**
 * Same shape but WITHOUT the marker interface — MicroserviceServer must
 * skip the auto-subscribe call for transports that don't implement it.
 */
final class PlainTransport implements ServerTransport
{
    public int $listenCalls = 0;

    public function connect(): void
    {
    }

    public function listen(callable $onPacket): void
    {
        $this->listenCalls++;
    }

    public function close(): void
    {
    }

    public function name(): string
    {
        return 'plain';
    }
}

final class AutoSubscriptionTest extends TestCase
{
    public function testMessagePatternsAndEventPatternsArePushedSeparately(): void
    {
        $transport = new RecordingSubscribableTransport();
        $server = new MicroserviceServer($transport, new HandlerRegistry());

        $server->registerControllers(AutoSubscribeFixtureController::class);
        $server->listen();

        // RPC vs event kinds end up in their own buckets — they have
        // different delivery semantics (worker-pool vs fan-out).
        $this->assertEqualsCanonicalizing(
            ['user.find', 'math.sum'],
            $transport->messageSubscriptions,
        );
        $this->assertEqualsCanonicalizing(
            ['user.created'],
            $transport->eventSubscriptions,
        );
    }

    public function testNonSubscribableTransportIsNotCalled(): void
    {
        $transport = new PlainTransport();
        $server = new MicroserviceServer($transport, new HandlerRegistry());

        $server->registerControllers(AutoSubscribeFixtureController::class);
        $server->listen();

        // The marker check is silent for non-subscribable transports: listen()
        // still proceeds normally without any subscribe() side effect.
        $this->assertSame(1, $transport->listenCalls);
    }

    public function testSubscriptionsMergeWithoutDuplicates(): void
    {
        $transport = new RecordingSubscribableTransport();
        $transport->subscribe(['user.find'], []); // pre-seeded as if constructor had it

        $server = new MicroserviceServer($transport, new HandlerRegistry());
        $server->registerControllers(AutoSubscribeFixtureController::class);
        $server->listen();

        $this->assertEqualsCanonicalizing(
            ['user.find', 'math.sum'],
            $transport->messageSubscriptions,
        );
        $this->assertEqualsCanonicalizing(
            ['user.created'],
            $transport->eventSubscriptions,
        );
    }
}
