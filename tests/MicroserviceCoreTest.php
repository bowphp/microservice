<?php

declare(strict_types=1);

namespace Bow\Microservice\Tests;

use Bow\Microservice\Client\ClientProxy;
use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Exception\RpcException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use Bow\Microservice\Consumer\HandlerRegistry;
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Consumer\MicroserviceServer;
use PHPUnit\Framework\TestCase;

final class FixtureController
{
    #[MessagePattern('echo')]
    public function echo(mixed $data): mixed { return $data; }

    #[MessagePattern('sum')]
    public function sum(mixed $data): int { return array_sum($data['n'] ?? []); }

    #[EventPattern('noop')]
    public function noop(mixed $data): void {}
}

/** Loopback transport that pipes the client straight into the server's handle(). */
final class LoopbackClient implements ClientTransport
{
    public function __construct(private MicroserviceServer $server) {}
    public function connect(): void {}
    public function send(Packet $p, float $t = 5.0): ResponsePacket { return $this->server->handle($p) ?? ResponsePacket::ok($p->id, null); }
    public function emit(Packet $p): void { $this->server->handle($p); }
    public function close(): void {}
    public function name(): string { return 'loopback'; }
}

final class NullServerTransport implements ServerTransport
{
    public function connect(): void {}
    public function listen(callable $onPacket): void {}
    public function close(): void {}
    public function name(): string { return 'null'; }
}

final class MicroserviceCoreTest extends TestCase
{
    private function makeProxy(): ClientProxy
    {
        $registry = new HandlerRegistry();
        $server = new MicroserviceServer(new NullServerTransport(), $registry);
        $server->registerControllers(FixtureController::class);
        return new ClientProxy(new LoopbackClient($server));
    }

    public function testRpcReturnsValue(): void
    {
        $this->assertSame(['a' => 1], $this->makeProxy()->send('echo', ['a' => 1]));
    }

    public function testRpcComputes(): void
    {
        $this->assertSame(6, $this->makeProxy()->send('sum', ['n' => [1, 2, 3]]));
    }

    public function testUnknownPatternThrows(): void
    {
        $this->expectException(RpcException::class);
        $this->makeProxy()->send('does.not.exist', []);
    }

    public function testEventReturnsNothing(): void
    {
        $this->makeProxy()->emit('noop', ['x' => 1]);
        $this->assertTrue(true); // no exception == pass
    }

    public function testPacketRoundTrip(): void
    {
        $p = Packet::message('p.q', ['k' => 'v']);
        $r = Packet::fromArray($p->toArray());
        $this->assertSame($p->id, $r->id);
        $this->assertSame('p.q', $r->pattern);
        $this->assertSame('v', $r->data['k']);
    }

    public function testJsonSerializerRoundTrip(): void
    {
        $s = new JsonSerializer();
        $this->assertSame(['x' => 1], $s->decode($s->encode(['x' => 1])));
    }
}
