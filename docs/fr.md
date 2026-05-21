# bowphp/microservice — Guide d'utilisation

Une couche microservice de style NestJS pour BowPHP. Écrivez des contrôleurs
avec des attributs, lancez un consommateur qui écoute sur un transport, et
appelez-les via un `ClientProxy`. Cinq transports partagent la même enveloppe
afin que le même handler fonctionne sans modification sur TCP, Redis,
RabbitMQ, Kafka ou gRPC.

> English: see [docs/en.md](en.md).

## Sommaire

- [Concepts](#concepts)
- [Installation](#installation)
- [Démarrage rapide](#démarrage-rapide)
- [Définir des handlers](#définir-des-handlers)
- [Lancer le consommateur](#lancer-le-consommateur)
- [Appeler depuis un autre service](#appeler-depuis-un-autre-service)
- [Détails par transport](#détails-par-transport)
  - [TCP](#tcp)
  - [Redis](#redis)
  - [RabbitMQ](#rabbitmq)
  - [Kafka](#kafka)
  - [gRPC](#grpc)
- [Configuration](#configuration)
- [Intégration BowPHP](#intégration-bowphp)
- [Sérialiseur personnalisé](#sérialiseur-personnalisé)
- [Erreurs et journalisation](#erreurs-et-journalisation)
- [Ajouter un nouveau transport](#ajouter-un-nouveau-transport)
- [Limites et notes](#limites-et-notes)

## Concepts

**Packet.** Chaque transport véhicule la même enveloppe :
`{ pattern, data, id, kind }`.

- `pattern` — chaîne sur laquelle le consommateur dispatche, ex. `"user.find"`.
- `data` — payload sérialisable en JSON.
- `id` — identifiant de corrélation (vide pour les événements).
- `kind` — `"message"` (RPC) ou `"event"` (sans réponse attendue).

**ResponsePacket.** Réponse RPC : `{ id, response, isDisposed, err }`. Le
champ `id` correspond à celui de la requête, permettant au client de
corréler la réponse.

**Dispatch par pattern.** Un consommateur enregistre des contrôleurs ; le
`HandlerRegistry` utilise la réflexion pour associer chaque méthode annotée
`#[MessagePattern('foo')]` ou `#[EventPattern('bar')]` à son pattern. À
l'arrivée d'un packet, le serveur recherche le pattern et invoque la méthode
avec `(mixed $data, Packet $packet)`.

**Contrats de transport.** Deux interfaces — `ServerTransport` et
`ClientTransport` — masquent les détails de protocole. Changer de transport
ne demande aucune modification des handlers.

## Installation

```bash
composer require bowphp/microservice
```

Pré-requis par transport :

| Transport | Pré-requis                                                        |
|-----------|-------------------------------------------------------------------|
| TCP       | `ext-sockets` (inclus dans la plupart des builds PHP)             |
| Redis     | `ext-redis` (phpredis)                                            |
| RabbitMQ  | `php-amqplib/php-amqplib` (déjà requis par le paquet)             |
| Kafka     | `ext-rdkafka`                                                     |
| gRPC      | `pecl install grpc && composer require grpc/grpc google/protobuf` |

## Démarrage rapide

**Contrôleur** — une classe PHP simple avec des méthodes annotées.

```php
namespace App\Consumers;

use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Message\Packet;

final class UserConsumer
{
    #[MessagePattern('user.find')]
    public function find(mixed $data): array
    {
        $id = (int) ($data['id'] ?? 0);
        return ['id' => $id, 'name' => "User #{$id}"];
    }

    #[EventPattern('user.created')]
    public function onCreated(mixed $data, Packet $packet): void
    {
        // envoyer un mail de bienvenue, écrire un log d'audit…
    }
}
```

**Enregistrer le contrôleur** dans `config/microservice.php` :

```php
'controllers' => [
    \App\Consumers\UserConsumer::class,
],
```

**Lancer le consommateur** :

```bash
php bow microservice:listen --transport=redis
```

**Appeler depuis un autre service** :

```php
use Bow\Microservice\Client\ClientFactory;

$proxy = ClientFactory::create('redis', ['host' => '127.0.0.1']);
$proxy->connect();

$user = $proxy->send('user.find', ['id' => 42]);  // RPC — bloque jusqu'à la réponse
$proxy->emit('user.created', ['id' => 99]);       // événement — retourne aussitôt
```

## Définir des handlers

Un contrôleur est une simple classe. La bibliothèque détecte les handlers
par réflexion.

```php
use Bow\Microservice\Consumer\MessagePattern;
use Bow\Microservice\Consumer\EventPattern;
use Bow\Microservice\Message\Packet;

final class OrderConsumer
{
    public function __construct(private OrderService $orders) {}

    #[MessagePattern('order.create')]
    public function create(mixed $data): array
    {
        $order = $this->orders->create($data);
        return ['id' => $order->id, 'total' => $order->total];
    }

    #[MessagePattern('order.find')]
    public function find(mixed $data, Packet $packet): ?array
    {
        // $packet expose l'id de corrélation, le pattern, le kind — utile pour le tracing
        return $this->orders->find((int) ($data['id'] ?? 0))?->toArray();
    }

    #[EventPattern('order.paid')]
    public function onPaid(mixed $data): void
    {
        $this->orders->markPaid((int) $data['id']);
    }
}
```

Règles essentielles :

- Le premier paramètre reçoit le payload décodé (`mixed`).
- Le deuxième paramètre (facultatif) reçoit le `Packet` brut.
- Les handlers RPC (`#[MessagePattern]`) retournent toute valeur sérialisable
  en JSON ; elle devient le champ `response` du `ResponsePacket`.
- Les handlers d'événements (`#[EventPattern]`) ne retournent rien.
- L'injection de dépendances dans le constructeur fonctionne lorsque le
  consommateur tourne sous Bow (les contrôleurs sont résolus via le conteneur).

## Lancer le consommateur

Le paquet enregistre la commande Bow `microservice:listen` :

```bash
# Utiliser entièrement config/microservice.php
php bow microservice:listen

# Surcharger le transport via la ligne de commande
php bow microservice:listen --transport=tcp --host=0.0.0.0 --port=3000

# Restreindre les contrôleurs chargés
php bow microservice:listen --controllers="App\Consumers\UserConsumer,App\Consumers\OrderConsumer"
```

Les options CLI surchargent `config/microservice.php`. Options disponibles :

| Option           | Utilisée par           | Valeur par défaut                    |
|------------------|------------------------|--------------------------------------|
| `--transport`    | tous                   | `config('microservice.transport')`   |
| `--controllers`  | tous                   | `config('microservice.controllers')` |
| `--host`         | tcp / redis / rabbitmq | spécifique au transport              |
| `--port`         | tcp / redis / rabbitmq | spécifique au transport              |
| `--password`     | redis / rabbitmq       | aucune                               |
| `--patterns`     | redis                  | vide                                 |
| `--queue`        | rabbitmq               | `bow_microservice`                   |
| `--user`         | rabbitmq               | `guest`                              |
| `--vhost`        | rabbitmq               | `/`                                  |
| `--topics`       | kafka                  | vide                                 |
| `--brokers`      | kafka                  | `127.0.0.1:9092`                     |
| `--group`        | kafka                  | `bow-microservice`                   |

Lancez plusieurs instances derrière un load balancer / superviseur pour la
concurrence. Chaque transport gère une seule connexion à la fois par processus
(voir [Limites et notes](#limites-et-notes)).

**Arrêt propre.** Quand `ext-pcntl` est disponible, la commande installe des
handlers SIGTERM / SIGINT qui appellent `$server->stop()` avant de sortir
avec le code `0`, ce qui permet à supervisord / systemd / Kubernetes de
drainer un consommateur proprement. Sans `ext-pcntl`, le processus se
termine simplement à la réception du signal.

## Appeler depuis un autre service

Utilisez `ClientFactory::create` pour construire un `ClientProxy` :

```php
use Bow\Microservice\Client\ClientFactory;

$proxy = ClientFactory::create('redis', [
    'host' => '127.0.0.1',
    'port' => 6379,
]);
$proxy->connect();

// RPC — bloque jusqu'à la réponse ou le timeout
$user = $proxy->send('user.find', ['id' => 42]);

// Événement — fire-and-forget
$proxy->emit('user.created', ['id' => 99]);
```

Si le consommateur lève une exception, `send()` la relance côté client sous la
forme `Bow\Microservice\Exception\RpcException`. Pour les événements, les
erreurs du handler sont avalées et journalisées côté serveur (aucun client à
notifier).

Dans une application Bow, ce même proxy est câblé automatiquement dans le
conteneur — voir [Intégration BowPHP](#intégration-bowphp).

## Détails par transport

### TCP

Trames JSON préfixées par leur longueur (4 octets big-endian + payload) sur
socket brut. Aucun broker requis.

```bash
php bow microservice:listen --transport=tcp --host=0.0.0.0 --port=3000
```

```php
$proxy = ClientFactory::create('tcp', ['host' => '127.0.0.1', 'port' => 3000]);
```

**Limites.** Une connexion à la fois par processus. En production : N workers
derrière un balancer TCP (HAProxy, nginx stream).

### Redis

Utilise `phpredis`. Les requêtes passent par pub/sub (un canal par pattern
enregistré) ; les réponses utilisent une liste Redis par requête, indexée
par l'`id` du packet (`bow:reply:<id>`). Le serveur fait `RPUSH` de la
réponse dans cette liste, le client la lit par `BLPOP`. Les listes mettent
les messages en file, donc plus de race « publish avant subscribe » — même
un serveur qui répond avant que le client soit prêt à lire ne perd pas de
messages.

```bash
php bow microservice:listen --transport=redis --patterns=user.find,user.created
```

```php
$proxy = ClientFactory::create('redis', ['host' => '127.0.0.1']);
```

L'option `--patterns` indique au consommateur les canaux Redis auxquels
s'abonner. Sans elle, le consommateur ne reçoit rien — il n'y a pas de
découverte automatique.

### RabbitMQ

Messagerie persistante via `php-amqplib`. Le RPC utilise les mécanismes AMQP
`reply_to` et `correlation_id`.

```bash
php bow microservice:listen --transport=rabbitmq \
    --queue=bow_microservice --user=guest --password=guest
```

```php
$proxy = ClientFactory::create('rabbitmq', [
    'queue' => 'bow_microservice',
    'host'  => '127.0.0.1',
]);
```

La file est déclarée automatiquement à la première connexion.

### Kafka

Streaming à haut débit via `rdkafka`. Approche identique à NestJS : un topic
de requêtes en entrée, un topic de réponses corrélé par un header
`kafka_correlationId`.

```bash
php bow microservice:listen --transport=kafka \
    --topics=user_events --group=users
```

```php
$proxy = ClientFactory::create('kafka', [
    'brokers' => '127.0.0.1:9092',
    'topic'   => 'user_events',
]);
```

Kafka n'a pas de RPC natif ; l'approche par reply-topic suit celle de Nest.
Pour du streaming d'événements pur, utilisez uniquement `emit()`.

### gRPC

**Client uniquement.** PHP n'a pas de serveur gRPC natif, implémentez donc le
serveur dans un langage qui en a (Go, Node, Rust, Java) en suivant
[proto/microservice.proto](../proto/microservice.proto). Le proto définit un
seul service :

```proto
service MessageService {
  rpc Send(MessageEnvelope) returns (MessageEnvelope);
  rpc Emit(MessageEnvelope) returns (Empty);
}

message MessageEnvelope {
  bytes payload = 1;   // Packet / ResponsePacket encodé en JSON
}
```

Le champ `payload` transporte exactement les mêmes octets JSON que les autres
transports, ce qui permet à vos handlers existants (en Go/Node/etc.) de
continuer à dispatcher par chaîne de pattern.

```php
$proxy = ClientFactory::create('grpc', [
    'host' => '127.0.0.1',
    'port' => 50051,
]);
$user = $proxy->send('user.find', ['id' => 42]);
```

Lève une `TransportException` lors de `connect()` si l'extension `grpc` n'est
pas installée.

## Configuration

`config/microservice.php` est la seule source de vérité :

```php
return [
    'transport' => app_env('MICROSERVICE_TRANSPORT', 'redis'),
    'timeout'   => (float) app_env('MICROSERVICE_TIMEOUT', 5.0),

    'controllers' => [
        \App\Consumers\UserConsumer::class,
        \App\Consumers\OrderConsumer::class,
    ],

    'tcp'      => ['host' => '127.0.0.1', 'port' => 3000],
    'redis'    => ['host' => '127.0.0.1', 'port' => 6379, 'password' => null],
    'rabbitmq' => ['host' => '127.0.0.1', 'port' => 5672, 'user' => 'guest', 'password' => 'guest', 'queue' => 'bow_microservice'],
    'kafka'    => ['brokers' => '127.0.0.1:9092', 'topic' => 'bow_microservice'],
    'grpc'     => ['host' => '127.0.0.1', 'port' => 50051],
];
```

En l'absence de ce fichier, le provider se rabat sur les variables
d'environnement : `MICROSERVICE_TRANSPORT`, `MICROSERVICE_HOST`,
`MICROSERVICE_PORT`, `MICROSERVICE_QUEUE`, `MICROSERVICE_BROKERS`, etc.

## Intégration BowPHP

Enregistrez le provider une seule fois :

```php
// app/Kernel.php
public function configurations(): array
{
    return [
        \Bow\Microservice\Bow\MicroserviceConfiguration::class,
    ];
}
```

Bindings exposés dans le conteneur :

- `Bow\Microservice\Client\ClientProxy::class` — à type-hint dans vos
  contrôleurs / services.
- `'microservice.client'` — alias chaîne pour `app('microservice.client')`.

Le proxy se connecte au boot afin qu'un transport mal configuré échoue
immédiatement plutôt qu'en pleine requête.

Dans un contrôleur Bow :

```php
use Bow\Microservice\Client\ClientProxy;

final class CheckoutController
{
    public function __construct(private ClientProxy $microservice) {}

    public function pay(int $orderId): Response
    {
        $this->microservice->emit('order.paid', ['id' => $orderId]);
        return response()->json(['ok' => true]);
    }
}
```

Les contrôleurs consommateurs sont eux aussi résolus via le conteneur, donc
l'injection par constructeur fonctionne exactement comme pour les
contrôleurs HTTP.

## Sérialiseur personnalisé

Le `JsonSerializer` par défaut convient à la majorité des cas. Pour passer à
msgpack / Protobuf / etc., implémentez
`Bow\Microservice\Contracts\Serializer` :

```php
use Bow\Microservice\Contracts\Serializer;

final class MsgpackSerializer implements Serializer
{
    public function encode(mixed $value): string
    {
        return msgpack_pack($value);
    }

    public function decode(string $payload): mixed
    {
        return msgpack_unpack($payload);
    }
}
```

Passez-le explicitement lors de la construction d'un transport (les factories
utilisent un sérialiseur JSON par défaut ; instanciez le transport directement
pour injecter le vôtre).

## Erreurs et journalisation

**RPC.** Une exception côté handler devient un `ResponsePacket` d'erreur ;
le client la relance comme `Bow\Microservice\Exception\RpcException` avec
le message d'origine. Le type d'exception est perdu (runtimes différents) ;
encodez les codes d'erreur dans le payload si vous avez besoin d'un
dispatch typé.

**Événements.** Aucun client à notifier, donc les erreurs du handler sont
avalées et journalisées via le logger PSR-3 optionnel passé à
`MicroserviceFactory::create`.

**Erreurs de transport.** Problèmes de connexion, packets mal formés, etc.
lèvent `Bow\Microservice\Exception\TransportException`. La commande console
attrape ces erreurs et affiche un message clair avant de sortir en code
non-zéro.

## Ajouter un nouveau transport

Un nouveau transport, c'est deux classes :

```php
namespace App\Transport;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

final class NatsServerTransport implements ServerTransport
{
    public function listen(callable $onPacket): void { /* … */ }
    public function reply(Packet $request, ResponsePacket $response): void { /* … */ }
    public function stop(): void { /* … */ }
    public function name(): string { return 'nats'; }
}

final class NatsClientTransport implements ClientTransport
{
    public function connect(): void { /* … */ }
    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket { /* … */ }
    public function emit(Packet $packet): void { /* … */ }
    public function close(): void { /* … */ }
    public function name(): string { return 'nats'; }
}
```

Enregistrez-les dans votre propre factory, ou ajoutez les cases à
`MicroserviceFactory` et `ClientFactory` si vous proposez un upstream.

## Limites et notes

- **Une connexion par processus consommateur.** Tous les transports serveur
  sont bloquants et traitent un packet à la fois. Lancez N copies derrière
  un load balancer pour la concurrence.
- **Kafka n'a pas de RPC natif.** L'approche reply-topic + correlation-id
  fonctionne mais ajoute de la latence. Préférez `emit()` pour les flux
  d'événements à haut débit.
- **gRPC est client uniquement.** Un serveur gRPC PHP nécessite RoadRunner
  ou Swoole, ce qui ne correspond pas au modèle mono-processus.
- **JSON par défaut.** Implémentez `Serializer` pour passer à un format
  binaire si la taille du payload compte.
- **Exceptions aplaties.** L'information de type est perdue à travers le
  fil ; encodez la sémantique d'erreur dans le payload si vous avez besoin
  d'erreurs typées.
