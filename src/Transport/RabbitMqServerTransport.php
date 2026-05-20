<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * RabbitMQ consumer (php-amqplib), AMQP 0-9-1.
 *
 * One durable queue carries all inbound packets. RPC uses the standard
 * direct-reply pattern: the request message carries reply_to (a queue) and
 * correlation_id; we publish the ResponsePacket to that reply_to with the same
 * correlation_id. Events arrive on the same queue with no reply_to and are
 * simply dispatched.
 */
final class RabbitMqServerTransport implements ServerTransport
{
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;
    private bool $running = false;

    public function __construct(
        private readonly string $queue,
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 5672,
        private readonly string $user = 'guest',
        private readonly string $password = 'guest',
        private readonly string $vhost = '/',
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly int $prefetch = 1,
    ) {
        if (!class_exists(AMQPStreamConnection::class)) {
            throw new TransportException('php-amqplib/php-amqplib is required for RabbitMqServerTransport.');
        }
    }

    public function connect(): void
    {
        $this->connection = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->user,
            $this->password,
            $this->vhost
        );
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($this->queue, false, true, false, false);
        $this->channel->basic_qos(0, $this->prefetch, false);
    }

    public function listen(callable $onPacket): void
    {
        if ($this->channel === null) {
            throw new TransportException('connect() must be called before listen().');
        }

        $this->running = true;

        $this->channel->basic_consume(
            $this->queue,
            '',
            false,
            false, // manual ack
            false,
            false,
            function (AMQPMessage $msg) use ($onPacket): void {
                $this->onMessage($msg, $onPacket);
            }
        );

        while ($this->running && $this->channel->is_consuming()) {
            try {
                $this->channel->wait(null, false, 0); // block until next frame
            } catch (\Throwable $e) {
                if ($this->running) {
                    throw new TransportException('AMQP wait failed: ' . $e->getMessage(), 0, $e);
                }
            }
        }
    }

    /** @param callable(Packet):?ResponsePacket $onPacket */
    private function onMessage(AMQPMessage $msg, callable $onPacket): void
    {
        try {
            $packet = Packet::fromArray($this->serializer->decode($msg->getBody()));
            $response = $onPacket($packet);

            $replyTo = $msg->has('reply_to') ? $msg->get('reply_to') : null;
            if ($response !== null && $replyTo) {
                $correlationId = $msg->has('correlation_id') ? $msg->get('correlation_id') : $packet->id;
                $reply = new AMQPMessage(
                    $this->serializer->encode($response->toArray()),
                    ['correlation_id' => $correlationId, 'content_type' => 'application/json']
                );
                $msg->getChannel()->basic_publish($reply, '', $replyTo);
            }

            $msg->ack();
        } catch (\Throwable) {
            // Reject without requeue to avoid poison-message loops.
            $msg->nack(false);
        }
    }

    public function close(): void
    {
        $this->running = false;
        try {
            $this->channel?->close();
        } catch (\Throwable) {
        }
        try {
            $this->connection?->close();
        } catch (\Throwable) {
        }
        $this->channel = null;
        $this->connection = null;
    }

    public function name(): string
    {
        return 'rabbitmq';
    }
}
