<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\ClientTransport;
use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Kafka producer (ext-rdkafka).
 *
 * RPC: produce the request to $requestTopic with headers pointing at a reply
 * topic + correlation id, then consume the reply topic until the matching
 * correlation id arrives or the timeout elapses. Events: produce only.
 *
 * The reply topic defaults to "<requestTopic>.reply" and must exist (or auto
 * topic creation must be enabled on the broker).
 */
final class KafkaClientTransport implements ClientTransport
{
    private ?\RdKafka\Producer $producer = null;
    private ?\RdKafka\KafkaConsumer $replyConsumer = null;
    private readonly string $replyTopic;
    private readonly string $replyGroupId;

    public function __construct(
        private readonly string $requestTopic,
        private readonly string $brokers = '127.0.0.1:9092',
        private readonly Serializer $serializer = new JsonSerializer(),
        ?string $replyTopic = null,
        ?string $replyGroupId = null,
    ) {
        if (!\extension_loaded('rdkafka')) {
            throw new TransportException('The "rdkafka" extension is required for KafkaClientTransport.');
        }
        $this->replyTopic = $replyTopic ?? ($requestTopic . '.reply');
        $this->replyGroupId = $replyGroupId ?? ('bow-ms-client-' . bin2hex(random_bytes(4)));
    }

    public function connect(): void
    {
        if ($this->producer !== null) {
            return;
        }
        $pconf = new \RdKafka\Conf();
        $pconf->set('metadata.broker.list', $this->brokers);
        $this->producer = new \RdKafka\Producer($pconf);

        $cconf = new \RdKafka\Conf();
        $cconf->set('group.id', $this->replyGroupId);
        $cconf->set('metadata.broker.list', $this->brokers);
        // We only want replies that arrive after we start waiting.
        $cconf->set('auto.offset.reset', 'latest');
        $this->replyConsumer = new \RdKafka\KafkaConsumer($cconf);
        $this->replyConsumer->subscribe([$this->replyTopic]);
    }

    public function send(Packet $packet, float $timeout = 5.0): ResponsePacket
    {
        $this->connect();

        $topic = $this->producer->newTopic($this->requestTopic);
        $topic->producev(
            RD_KAFKA_PARTITION_UA,
            0,
            $this->serializer->encode($packet->toArray()),
            $packet->id,
            [
                'kafka_replyTopic'    => $this->replyTopic,
                'kafka_correlationId' => $packet->id,
            ]
        );
        $this->producer->poll(0);
        $this->producer->flush(2000);

        $deadline = microtime(true) + $timeout;
        while (microtime(true) < $deadline) {
            $remainingMs = (int) max(10, ($deadline - microtime(true)) * 1000);
            $message = $this->replyConsumer->consume($remainingMs);

            if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                $headers = $message->headers ?? [];
                $cid = $headers['kafka_correlationId'] ?? $message->key;
                if ((string) $cid === $packet->id) {
                    return ResponsePacket::fromArray($this->serializer->decode((string) $message->payload));
                }
            }
            // Timeouts / EOF: keep polling until the deadline.
        }

        throw new TransportException("Kafka RPC for '{$packet->pattern}' timed out after {$timeout}s.");
    }

    public function emit(Packet $packet): void
    {
        $this->connect();
        $topic = $this->producer->newTopic($this->requestTopic);
        $topic->producev(
            RD_KAFKA_PARTITION_UA,
            0,
            $this->serializer->encode($packet->toArray()),
            $packet->id
        );
        $this->producer->poll(0);
        $this->producer->flush(2000);
    }

    public function close(): void
    {
        try {
            $this->replyConsumer?->close();
        } catch (\Throwable) {
        }
        $this->replyConsumer = null;
        $this->producer = null;
    }

    public function name(): string
    {
        return 'kafka';
    }
}
