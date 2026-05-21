<?php

declare(strict_types=1);

namespace Bow\Microservice\Consumer;

use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Exception\HandlerNotFoundException;
use Bow\Microservice\Message\Packet;

/**
 * Maps patterns -> callables. Controllers are plain PHP classes whose methods
 * carry #[MessagePattern] / #[EventPattern] attributes. Instances are created
 * lazily through a resolver (so a DI container can be plugged in), defaulting
 * to plain `new`.
 */
final class HandlerRegistry
{
    /** @var array<string, array{class:class-string, method:string, event:bool}> */
    private array $handlers = [];

    /** @var callable(class-string):object */
    private $resolver;

    /** @var array<class-string, object> */
    private array $instances = [];

    public function __construct(?callable $resolver = null)
    {
        $this->resolver = $resolver ?? static fn (string $class): object => new $class();
    }

    /**
     * Register every attributed method on a controller class.
     *
     * @param class-string $controller
     */
    public function registerController(string $controller): void
    {
        $ref = new \ReflectionClass($controller);

        foreach ($ref->getMethods(\ReflectionMethod::IS_PUBLIC) as $method) {
            foreach ($method->getAttributes(MessagePattern::class) as $attr) {
                /** @var MessagePattern $instance */
                $instance = $attr->newInstance();
                $this->bind($instance->pattern, $controller, $method->getName(), false);
            }
            foreach ($method->getAttributes(EventPattern::class) as $attr) {
                /** @var EventPattern $instance */
                $instance = $attr->newInstance();
                $this->bind($instance->pattern, $controller, $method->getName(), true);
            }
        }
    }

    /** Register a raw closure handler (no controller class needed). */
    public function on(string $pattern, callable $handler, bool $event = false): void
    {
        $this->handlers[$pattern] = ['closure' => $handler, 'event' => $event];
    }

    public function has(string $pattern): bool
    {
        return isset($this->handlers[$pattern]);
    }

    /** @return list<string> */
    public function patterns(): array
    {
        return array_keys($this->handlers);
    }

    /**
     * Invoke the handler for a packet. Returns whatever the handler returns
     * (the value that will become the RPC response), or null for events.
     */
    public function dispatch(Packet $packet): mixed
    {
        $h = $this->handlers[$packet->pattern]
            ?? throw new HandlerNotFoundException("No handler registered for pattern '{$packet->pattern}'.");

        if (isset($h['closure'])) {
            return ($h['closure'])($packet->data, $packet);
        }

        $instance = $this->instances[$h['class']]
            ??= ($this->resolver)($h['class']);

        return $instance->{$h['method']}($packet->data, $packet);
    }

    public function isEventPattern(string $pattern): bool
    {
        return (bool) ($this->handlers[$pattern]['event'] ?? false);
    }

    private function bind(string $pattern, string $class, string $method, bool $event): void
    {
        $this->handlers[$pattern] = ['class' => $class, 'method' => $method, 'event' => $event];
    }
}
