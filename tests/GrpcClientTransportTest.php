<?php

declare(strict_types=1);

namespace Bow\Microservice\Tests;

use Bow\Microservice\Client\ClientFactory;
use Bow\Microservice\Exception\RpcException;
use Bow\Microservice\Message\Packet;
use Bow\Microservice\Message\ResponsePacket;
use Bow\Microservice\Transport\Grpc\MessageEnvelope;
use Bow\Microservice\Transport\Grpc\ProtobufCodec;
use Bow\Microservice\Transport\GrpcClientTransport;
use PHPUnit\Framework\TestCase;

/**
 * Stand-in for Grpc\UnaryCall::wait(). The test controls what wait() returns.
 */
final class FakeUnaryCall
{
    public function __construct(
        private readonly ?MessageEnvelope $reply,
        private readonly int $code = 0,
        private readonly string $details = '',
    ) {
    }

    public function wait(): array
    {
        return [$this->reply, (object) ['code' => $this->code, 'details' => $this->details]];
    }
}

/**
 * Duck-typed gRPC stub. Records last Send/Emit args, returns pre-canned calls.
 */
final class FakeMessageServiceClient
{
    public ?MessageEnvelope $lastSent = null;
    public ?MessageEnvelope $lastEmitted = null;
    public array $lastSendOptions = [];

    public function __construct(private FakeUnaryCall $sendResult, private FakeUnaryCall $emitResult)
    {
    }

    public function Send(MessageEnvelope $env, array $metadata = [], array $options = []): FakeUnaryCall
    {
        $this->lastSent = $env;
        $this->lastSendOptions = $options;
        return $this->sendResult;
    }

    public function Emit(MessageEnvelope $env, array $metadata = [], array $options = []): FakeUnaryCall
    {
        $this->lastEmitted = $env;
        return $this->emitResult;
    }
}

final class GrpcClientTransportTest extends TestCase
{
    public function testProtobufCodecRoundTripsJsonPayload(): void
    {
        $payload = json_encode(['pattern' => 'user.find', 'data' => ['id' => 42]]);

        $bytes = ProtobufCodec::encodeBytesField($payload);
        $back = ProtobufCodec::decodeBytesField($bytes);

        $this->assertSame($payload, $back);
        // Tag byte for field 1, wire type 2 = (1 << 3) | 2 = 0x0A
        $this->assertSame(0x0A, ord($bytes[0]));
    }

    public function testProtobufCodecHandlesEmptyEnvelope(): void
    {
        // The Emit RPC's reply is an empty MessageEnvelope (no payload field).
        $this->assertSame('', ProtobufCodec::decodeBytesField(''));
    }

    public function testProtobufCodecEncodesLongPayloadsWithMultiByteVarint(): void
    {
        $payload = str_repeat('x', 300); // length 300 needs a 2-byte varint
        $bytes = ProtobufCodec::encodeBytesField($payload);

        $this->assertSame($payload, ProtobufCodec::decodeBytesField($bytes));
        $this->assertSame(0x0A, ord($bytes[0]));
        // 300 = 0xAC 0x02 in little-endian base-128
        $this->assertSame(0xAC, ord($bytes[1]));
        $this->assertSame(0x02, ord($bytes[2]));
    }

    public function testSendRoundTripsPacketThroughEnvelope(): void
    {
        $reply = new MessageEnvelope(json_encode([
            'id'         => 'corr-1',
            'response'   => ['ok' => true],
            'isDisposed' => true,
            'err'        => null,
        ]));
        $stub = new FakeMessageServiceClient(new FakeUnaryCall($reply), new FakeUnaryCall(null));

        $transport = new GrpcClientTransport(injectedStub: $stub);
        $packet = Packet::message('user.find', ['id' => 42], 'corr-1');

        $response = $transport->send($packet, 2.5);

        $this->assertInstanceOf(ResponsePacket::class, $response);
        $this->assertSame('corr-1', $response->id);
        $this->assertSame(['ok' => true], $response->response);

        $sentDecoded = json_decode($stub->lastSent->payload, true);
        $this->assertSame('user.find', $sentDecoded['pattern']);
        $this->assertSame(['id' => 42], $sentDecoded['data']);
        $this->assertSame(2_500_000, $stub->lastSendOptions['timeout']);
    }

    public function testSendThrowsRpcExceptionOnNonZeroStatus(): void
    {
        $stub = new FakeMessageServiceClient(
            new FakeUnaryCall(null, code: 14, details: 'UNAVAILABLE'),
            new FakeUnaryCall(null),
        );

        $transport = new GrpcClientTransport(injectedStub: $stub);

        $this->expectException(RpcException::class);
        $this->expectExceptionMessage('gRPC call failed (code=14): UNAVAILABLE');

        $transport->send(Packet::message('user.find', []));
    }

    public function testEmitSendsEnvelopeAndIgnoresReplyBody(): void
    {
        $stub = new FakeMessageServiceClient(
            new FakeUnaryCall(null),
            new FakeUnaryCall(null), // empty reply; status code 0
        );

        $transport = new GrpcClientTransport(injectedStub: $stub);
        $transport->emit(Packet::event('user.created', ['id' => 7]));

        $this->assertNotNull($stub->lastEmitted);
        $decoded = json_decode($stub->lastEmitted->payload, true);
        $this->assertSame('user.created', $decoded['pattern']);
        $this->assertSame(['id' => 7], $decoded['data']);
    }

    public function testTransportNameIsGrpc(): void
    {
        $stub = new FakeMessageServiceClient(new FakeUnaryCall(null), new FakeUnaryCall(null));

        $this->assertSame('grpc', (new GrpcClientTransport(injectedStub: $stub))->name());
    }

    public function testClientFactoryWiresGrpcTransport(): void
    {
        // ClientFactory uses the real (no-stub) constructor path; ensure it
        // builds without touching the extension at construct time. connect()
        // is what would throw if grpc is missing — we don't call it here.
        $proxy = ClientFactory::create(ClientFactory::GRPC, ['host' => '10.0.0.1', 'port' => 50000]);

        $this->assertNotNull($proxy);
    }
}
