<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * RabbitMQ producer (php-amqplib).
 *
 * RPC: declare an exclusive temporary reply queue, publish the request with
 * reply_to set to it and a correlation_id, then wait for the matching reply.
 * Events: publish to the queue with no reply_to.
 */
final class RabbitMqClientTransport implements ClientTransport
{
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;
    private ?string $replyQueue = null;

    public function __construct(
        private readonly string $queue,
        private readonly string $host = '127.0.0.1',
        private readonly int $port = 5672,
        private readonly string $user = 'guest',
        private readonly string $password = 'guest',
        private readonly string $vhost = '/',
        private readonly Serializer $serializer = new JsonSerializer(),
    ) {
        if (!class_exists(AMQPStreamConnection::class)) {
            throw new TransportException('php-amqplib/php-amqplib is required for RabbitMqClientTransport.');
        }
    }

    public function connect(): void
    {
        if ($this->channel !== null) {
            return;
        }
        $this->connection = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->user,
            $this->password,
            $this->vhost
        );
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($this->queue, false, true, false, false);

        // Exclusive, auto-named, auto-deleted reply queue for this client.
        [$this->replyQueue] = $this->channel->queue_declare('', false, false, true, false);
    }

    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket
    {
        $this->connect();

        $captured = null;
        $consumerTag = $this->channel->basic_consume(
            $this->replyQueue,
            '',
            false,
            true, // auto-ack replies
            true, // exclusive
            false,
            function (AMQPMessage $msg) use (&$captured, $packet): void {
                $cid = $msg->has('correlation_id') ? $msg->get('correlation_id') : null;
                if ($cid !== null && $cid !== $packet->id) {
                    return;
                }
                $captured = ResponsePacket::fromArray($this->serializer->decode($msg->getBody()));
            }
        );

        $message = new AMQPMessage(
            $this->serializer->encode($packet->toArray()),
            [
                'correlation_id' => $packet->id,
                'reply_to'       => $this->replyQueue,
                'content_type'   => 'application/json',
                'delivery_mode'  => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]
        );
        $this->channel->basic_publish($message, '', $this->queue);

        $deadline = microtime(true) + $timeout;
        while ($captured === null && microtime(true) < $deadline) {
            try {
                $this->channel->wait(null, false, max(0.01, $deadline - microtime(true)));
            } catch (\PhpAmqpLib\Exception\AMQPTimeoutException) {
                break;
            }
        }

        try {
            $this->channel->basic_cancel($consumerTag);
        } catch (\Throwable) {
        }

        if ($captured === null) {
            throw new TransportException("RabbitMQ RPC for '{$packet->pattern}' timed out after {$timeout}s.");
        }

        return $captured;
    }

    public function emit(Packet $packet): void
    {
        $this->connect();
        $message = new AMQPMessage(
            $this->serializer->encode($packet->toArray()),
            ['content_type' => 'application/json', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]
        );
        $this->channel->basic_publish($message, '', $this->queue);
    }

    public function close(): void
    {
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
