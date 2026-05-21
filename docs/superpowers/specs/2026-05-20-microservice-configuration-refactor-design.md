# Refactor `MicroserviceConfiguration` to Bow's `Configuration` provider contract

**Date:** 2026-05-20
**Scope:** `src/Bow/MicroserviceConfiguration.php` (single file)
**Status:** Design approved by user

## Problem

`Bow\Microservice\Bow\MicroserviceConfiguration` currently extends `Bow\Configuration\Loader` (aliased — with a typo — as `BowConfigirationLoader`). That is the wrong base class:

- `Bow\Configuration\Loader` is the **application kernel** (singleton via `Loader::configure($basePath)`). Apps extend it once in `app/Kernel.php` to declare `configurations()`, `middlewares()`, `namespaces()`, `events()`. It is not a service provider.
- `Bow\Configuration\Configuration` is the abstract **service provider** base. The container (`Bow\Container\Capsule`) is injected in its constructor; the lifecycle is `create(Loader $config): void` then `run(): void`. Classes listed in the host app's `Kernel::configurations()` array must extend this.

Because the wrong base was chosen, the class invented its own lifecycle (`process()` / `register()` / `boot()`) and a `setBinder()` + container-probing fallback to find a container at runtime — all of which become unnecessary once the canonical contract is used.

## Reference pattern

`Bow\Queue\QueueConfiguration` is the minimal canonical example:

```php
class QueueConfiguration extends Configuration
{
    public function create(Loader $config): void
    {
        $this->container->bind('queue', function () use ($config) {
            return new QueueConnection($config['worker'] ?? $config['queue']);
        });
    }

    public function run(): void
    {
        $this->container->make('queue');
    }
}
```

Key contract points:
- Container is `$this->container` (always available; injected via constructor on `Configuration`).
- `create()` registers bindings.
- `run()` boots / eagerly resolves to fail fast.

## Target shape

```php
namespace Bow\Microservice\Bow;

use Bow\Configuration\Configuration;
use Bow\Configuration\Loader;
use Bow\Microservice\Client\ClientFactory;
use Bow\Microservice\Client\ClientProxy;

final class MicroserviceConfiguration extends Configuration
{
    public function create(Loader $config): void
    {
        $settings = $this->resolveSettings($config);

        $factory = static function () use ($settings): ClientProxy {
            $proxy = ClientFactory::create(
                $settings['transport'],
                $settings['options'],
                $settings['timeout'],
            );
            $proxy->connect();
            return $proxy;
        };

        $this->container->bind(ClientProxy::class, $factory);
        $this->container->bind('microservice.client', $factory);
    }

    public function run(): void
    {
        $this->container->make('microservice.client');
    }

    /**
     * @return array{transport:string, options:array<string,mixed>, timeout:float}
     */
    private function resolveSettings(Loader $config): array
    {
        $cfg = $config['microservice'] ?? null;
        if (is_array($cfg) && isset($cfg['transport'])) {
            return [
                'transport' => (string) $cfg['transport'],
                'options'   => (array)  ($cfg['options'] ?? []),
                'timeout'   => (float)  ($cfg['timeout'] ?? 5.0),
            ];
        }

        $transport = getenv('MICROSERVICE_TRANSPORT') ?: 'redis';
        return [
            'transport' => $transport,
            'options'   => self::optionsFromEnv($transport),
            'timeout'   => (float) (getenv('MICROSERVICE_TIMEOUT') ?: 5.0),
        ];
    }

    /** @return array<string,mixed> */
    private static function optionsFromEnv(string $transport): array
    {
        return match ($transport) {
            'tcp' => [
                'host' => getenv('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port' => (int) (getenv('MICROSERVICE_PORT') ?: 3000),
            ],
            'redis' => [
                'host'     => getenv('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port'     => (int) (getenv('MICROSERVICE_PORT') ?: 6379),
                'password' => getenv('MICROSERVICE_REDIS_PASSWORD') ?: null,
            ],
            'rabbitmq' => [
                'host'     => getenv('MICROSERVICE_HOST') ?: '127.0.0.1',
                'port'     => (int) (getenv('MICROSERVICE_PORT') ?: 5672),
                'user'     => getenv('MICROSERVICE_RABBIT_USER') ?: 'guest',
                'password' => getenv('MICROSERVICE_RABBIT_PASSWORD') ?: 'guest',
                'queue'    => getenv('MICROSERVICE_QUEUE') ?: 'bow_microservice',
            ],
            'kafka' => [
                'brokers' => getenv('MICROSERVICE_BROKERS') ?: '127.0.0.1:9092',
                'topic'   => getenv('MICROSERVICE_TOPIC') ?: 'bow_microservice',
            ],
            default => [],
        };
    }
}
```

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Base class | `Bow\Configuration\Configuration` | Canonical service provider contract in Bow. |
| `run()` behavior | Eager: `make('microservice.client')` | Mirrors `QueueConfiguration::run()`. Fails fast at boot if transport is misconfigured rather than mid-request. |
| Config source | `$config['microservice']` first, env fallback | Uses the injected `Loader` (ArrayAccess) directly; consistent with how other Bow `Configuration` classes read settings. Drops the global `\config()` helper call. |
| Bindings | `ClientProxy::class` + `'microservice.client'` | Preserves both binding keys the current code exposes, so consumers don't break. |

## Removed

- `BowConfigirationLoader` alias (and its typo).
- `extends Loader` — wrong base class.
- `$binder` property + `setBinder()` — `$this->container` is always available.
- `process()`, `register()`, `boot()` — not part of Bow's `Configuration` contract.
- `bindViaBowContainer()` and the `class_exists` probing of `Capsule` / `Container` — replaced by direct calls on `$this->container`.
- Public static `config()` — replaced by private instance `resolveSettings(Loader)`. Callers that used `MicroserviceConfiguration::config()` directly must migrate to reading from the host app's `config('microservice')` helper or resolve `microservice.client` from the container.

## Kept

- `optionsFromEnv()` — pure helper, no contract changes.
- The transport/options/timeout configuration shape (no behavior change for callers).
- The package-doc commentary at the top of the class (rewritten to reflect the new contract).

## Host-app registration (unchanged for consumers, simpler)

```php
// app/Kernel.php
public function configurations(): array
{
    return [
        \Bow\Microservice\Bow\MicroserviceConfiguration::class,
    ];
}
```

The host app no longer needs `setBinder()` plumbing — Bow's `Loader::boot()` instantiates the provider with `$this->container = Capsule::getInstance()`.

## Risks / out of scope

- **Out of scope:** changes to `ClientFactory`, `ClientProxy`, transport implementations.
- **Test impact:** None. `tests/MicroserviceCoreTest.php` exercises `ClientProxy`, packets, and the server only — it does not touch `MicroserviceConfiguration`.
- **BC note:** The public methods `process()`, `register()`, `boot()`, `setBinder()`, and `config()` are removed. Any external code calling them must migrate to the standard Bow registration path. This is intentional — the previous shape was a workaround, not a supported API.

## Additional findings discovered after initial design

1. **Config shape mismatch.** The actual `config/microservice.php` uses per-transport keys (`'tcp' => [...]`, `'redis' => [...]`, `'rabbitmq' => [...]`, `'kafka' => [...]`) — NOT a single `'options'` array. The current code's `config()` reads `$cfg['options']` and will always miss. The refactored `resolveSettings()` must read the transport-specific block: `$cfg[$transport] ?? []`.

2. **Stale docblock in `config/microservice.php`.** Top comment says "Read by `MicroserviceServiceProvider::config()`" — no such class. Update to "Read by `MicroserviceConfiguration::create()`".

3. **Cleanup opportunity in `config/microservice.php` (in scope as part of this refactor since we are aligning config + provider):**
   - The `redis` block contains a leftover `'user'` and `'queue'` — neither belongs to a Redis client; remove.
   - All transports default to port `6379` (Redis's port), even `tcp` (which the env-fallback path defaulted to `3000`) and `rabbitmq` (default `5672`) and `kafka` (default `9092`). Set sane per-transport port defaults consistent with the env-fallback values in `optionsFromEnv()`.

These three changes are added to the implementation scope.
