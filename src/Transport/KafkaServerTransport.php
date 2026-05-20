<?php

declare(strict_types=1);

namespace Bow\Microservice\Transport;

use Bow\Microservice\Contracts\Serializer;
use Bow\Microservice\Contracts\ServerTransport;
use Bow\Microservice\Exception\TransportException;
use Bow\Microservice\Message\JsonSerializer;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;

/**
 * Kafka consumer (ext-rdkafka).
 *
 * Kafka has no built-in request/response, so — exactly like NestJS — we model
 * RPC with a reply topic. The inbound message carries headers:
 *   kafka_replyTopic   : where to produce the response
 *   kafka_correlationId: echoed back so the client can match it
 * Events simply omit those headers. The consumer subscribes to one request
 * topic (typically named after the pattern or a shared topic) and produces
 * replies through a short-lived producer.
 */
final class KafkaServerTransport implements ServerTransport
{
    private ?\RdKafka\KafkaConsumer $consumer = null;
    private ?\RdKafka\Producer $producer = null;
    private bool $running = false;

    /**
     * @param list<string> $topics request topics to subscribe to
     */
    public function __construct(
        private readonly array $topics,
        private readonly string $groupId,
        private readonly string $brokers = '127.0.0.1:9092',
        private readonly Serializer $serializer = new JsonSerializer(),
        private readonly int $pollTimeoutMs = 1000,
    ) {
        if (!\extension_loaded('rdkafka')) {
            throw new TransportException('The "rdkafka" extension is required for KafkaServerTransport.');
        }
        if ($topics === []) {
            throw new TransportException('KafkaServerTransport needs at least one topic.');
        }
    }

    public function connect(): void
    {
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', $this->groupId);
        $conf->set('metadata.broker.list', $this->brokers);
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.auto.commit', 'true');

        $this->consumer = new \RdKafka\KafkaConsumer($conf);
        $this->consumer->subscribe($this->topics);

        $pconf = new \RdKafka\Conf();
        $pconf->set('metadata.broker.list', $this->brokers);
        $this->producer = new \RdKafka\Producer($pconf);
    }

    public function listen(callable $onPacket): void
    {
        if ($this->consumer === null) {
            throw new TransportException('connect() must be called before listen().');
        }

        $this->running = true;

        while ($this->running) {
            $message = $this->consumer->consume($this->pollTimeoutMs);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->onMessage($message, $onPacket);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break; // idle poll, keep looping
                default:
                    if ($this->running) {
                        throw new TransportException('Kafka consume error: ' . $message->errstr());
                    }
            }
        }
    }

    /** @param callable(Packet):?ResponsePacket $onPacket */
    private function onMessage(\RdKafka\Message $message, callable $onPacket): void
    {
        try {
            $packet = Packet::fromArray($this->serializer->decode((string) $message->payload));
        } catch (\Throwable) {
            return;
        }

        $response = $onPacket($packet);
        if ($response === null) {
            return; // event
        }

        $headers = $message->headers ?? [];
        $replyTopic = $headers['kafka_replyTopic'] ?? null;
        $correlationId = $headers['kafka_correlationId'] ?? $packet->id;

        if ($replyTopic) {
            $topic = $this->producer->newTopic((string) $replyTopic);
            $topic->producev(
                RD_KAFKA_PARTITION_UA,
                0,
                $this->serializer->encode($response->toArray()),
                (string) $correlationId,
                ['kafka_correlationId' => (string) $correlationId]
            );
            $this->producer->poll(0);
            $this->producer->flush(2000);
        }
    }

    public function close(): void
    {
        $this->running = false;
        try {
            $this->consumer?->close();
        } catch (\Throwable) {
        }
        $this->consumer = null;
        $this->producer = null;
    }

    public function name(): string
    {
        return 'kafka';
    }
}
