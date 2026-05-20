<?php

declare(strict_types=1);

/*
| client.php — the caller side (ClientProxy).
|
|   php client.php --transport=redis
|
| Run a consumer first (microservice.php) on the same transport.
*/

use Bow\Microservice\Client\ClientFactory;

require __DIR__ . '/vendor/autoload.php';

$opts = getopt('', ['transport:', 'host:', 'port:']);
$transport = $opts['transport'] ?? 'redis';

$proxy = ClientFactory::create($transport, [
    'host'  => $opts['host'] ?? '127.0.0.1',
    'port'  => isset($opts['port']) ? (int) $opts['port'] : null,
    'topic' => 'user_events',
    'queue' => 'bow_microservice',
]);
$proxy->connect();

// RPC — blocks for the reply.
$user = $proxy->send('user.find', ['id' => 42]);
echo "user.find  => " . json_encode($user) . PHP_EOL;

$sum = $proxy->send('math.sum', ['numbers' => [1, 2, 3, 4]]);
echo "math.sum   => {$sum}" . PHP_EOL;

// Event — fire-and-forget.
$proxy->emit('user.created', ['id' => 99, 'name' => 'Ada']);
echo "user.created emitted" . PHP_EOL;

$proxy->close();
