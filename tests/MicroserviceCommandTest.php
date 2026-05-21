<?php

declare(strict_types=1);

namespace Bow\Microservice\Tests;

use Bow\Console\Argument;
use Bow\Console\Setting;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Console\MicroserviceCommand;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;

final class FakeAutoDiscoveryController
{
    #[MessagePattern('user.find')]
    public function find(): array
    {
        return [];
    }

    #[MessagePattern('order.create')]
    public function create(): array
    {
        return [];
    }

    #[EventPattern('user.created')]
    public function onCreated(): void
    {
    }
}

final class MicroserviceCommandTest extends TestCase
{
    /**
     * @param list<string> $argv
     */
    private function buildCommand(array $argv): MicroserviceCommand
    {
        $GLOBALS['argv'] = array_merge(['php', 'microservice:listen'], $argv);

        return new MicroserviceCommand(new Setting(sys_get_temp_dir()), new Argument());
    }

    private function invokePrivate(MicroserviceCommand $cmd, string $method, mixed ...$args): mixed
    {
        return (new ReflectionMethod($cmd, $method))->invoke($cmd, ...$args);
    }

    public function testResolveControllersFromCli(): void
    {
        $cmd = $this->buildCommand(['--controllers=App\Foo,App\Bar']);

        $this->assertSame(
            ['App\Foo', 'App\Bar'],
            $this->invokePrivate($cmd, 'resolveControllers'),
        );
    }

    public function testResolveTcpOptionsFromCli(): void
    {
        $cmd = $this->buildCommand(['--host=0.0.0.0', '--port=4000']);

        $this->assertSame(
            ['host' => '0.0.0.0', 'port' => 4000],
            $this->invokePrivate($cmd, 'resolveOptions', 'tcp'),
        );
    }

    public function testResolveRedisOptionsWithPatterns(): void
    {
        $cmd = $this->buildCommand(['--host=10.0.0.1', '--patterns=user.find,user.created']);

        $opts = $this->invokePrivate($cmd, 'resolveOptions', 'redis');

        $this->assertSame('10.0.0.1', $opts['host']);
        $this->assertSame(6379, $opts['port']);
        $this->assertSame(['user.find', 'user.created'], $opts['patterns']);
    }

    public function testRedisPatternsAreAutoDiscoveredFromControllerAttributes(): void
    {
        $cmd = $this->buildCommand([]);

        $opts = $this->invokePrivate(
            $cmd,
            'resolveOptions',
            'redis',
            [FakeAutoDiscoveryController::class],
        );

        $this->assertEqualsCanonicalizing(
            ['user.find', 'order.create', 'user.created'],
            $opts['patterns'],
        );
    }

    public function testCliPatternsOverrideAutoDiscovery(): void
    {
        $cmd = $this->buildCommand(['--patterns=only.this']);

        $opts = $this->invokePrivate(
            $cmd,
            'resolveOptions',
            'redis',
            [FakeAutoDiscoveryController::class],
        );

        $this->assertSame(['only.this'], $opts['patterns']);
    }

    public function testResolveRabbitMqOptionsFromCli(): void
    {
        $cmd = $this->buildCommand([
            '--queue=jobs',
            '--user=bow',
            '--password=secret',
        ]);

        $opts = $this->invokePrivate($cmd, 'resolveOptions', 'rabbitmq');

        $this->assertSame('jobs', $opts['queue']);
        $this->assertSame('bow', $opts['user']);
        $this->assertSame('secret', $opts['password']);
    }

    public function testResolveKafkaOptionsFromCli(): void
    {
        $cmd = $this->buildCommand([
            '--brokers=kafka:9092',
            '--topics=events,audit',
            '--group=consumers',
        ]);

        $opts = $this->invokePrivate($cmd, 'resolveOptions', 'kafka');

        $this->assertSame('kafka:9092', $opts['brokers']);
        $this->assertSame(['events', 'audit'], $opts['topics']);
        $this->assertSame('consumers', $opts['group_id']);
    }

    public function testUnknownTransportYieldsEmptyOptions(): void
    {
        $cmd = $this->buildCommand([]);

        $this->assertSame([], $this->invokePrivate($cmd, 'resolveOptions', 'nats'));
    }
}
