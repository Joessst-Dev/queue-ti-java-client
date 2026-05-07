package de.joesst.dev.queueti.spring.integration;

import de.joesst.dev.queueti.QueueTiClient;
import de.joesst.dev.queueti.QueueTiClientTestFixture;
import de.joesst.dev.queueti.pb.AckRequest;
import de.joesst.dev.queueti.pb.AckResponse;
import de.joesst.dev.queueti.pb.NackRequest;
import de.joesst.dev.queueti.pb.NackResponse;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import de.joesst.dev.queueti.pb.SubscribeRequest;
import de.joesst.dev.queueti.pb.SubscribeResponse;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class QueueTiInboundChannelAdapterTest {

    // =========================================================================
    // Fake gRPC service
    // =========================================================================

    private static class FakeService extends QueueServiceGrpc.QueueServiceImplBase {

        private final List<SubscribeResponse> responses;
        volatile CountDownLatch ackLatch = new CountDownLatch(1);
        volatile CountDownLatch nackLatch = new CountDownLatch(1);
        final List<AckRequest> capturedAcks = new ArrayList<>();
        final List<NackRequest> capturedNacks = new ArrayList<>();
        // Kept open to avoid triggering reconnect during the test
        volatile StreamObserver<SubscribeResponse> openObserver;

        FakeService(final List<SubscribeResponse> responses) {
            this.responses = responses;
        }

        @Override
        public synchronized void subscribe(
                final SubscribeRequest request,
                final StreamObserver<SubscribeResponse> responseObserver) {
            openObserver = responseObserver;
            for (final SubscribeResponse r : responses) {
                responseObserver.onNext(r);
            }
            // Leave stream open — do not call onCompleted() — so the consumer does not
            // immediately reconnect while the test's handler thread is still running.
        }

        @Override
        public synchronized void ack(
                final AckRequest request,
                final StreamObserver<AckResponse> responseObserver) {
            capturedAcks.add(request);
            responseObserver.onNext(AckResponse.getDefaultInstance());
            responseObserver.onCompleted();
            ackLatch.countDown();
        }

        @Override
        public synchronized void nack(
                final NackRequest request,
                final StreamObserver<NackResponse> responseObserver) {
            capturedNacks.add(request);
            responseObserver.onNext(NackResponse.getDefaultInstance());
            responseObserver.onCompleted();
            nackLatch.countDown();
        }
    }

    // =========================================================================
    // Test infrastructure
    // =========================================================================

    private static final String SERVER_NAME = "test-server";
    private static final String TOPIC = "orders";
    private static final String MESSAGE_ID = "msg-001";
    private static final byte[] PAYLOAD = "hello".getBytes();

    private Server server;
    private FakeService fakeService;
    private QueueTiClient client;
    private GenericApplicationContext context;
    private QueueTiInboundChannelAdapter adapter;

    private static SubscribeResponse sampleResponse() {
        return SubscribeResponse.newBuilder()
                .setId(MESSAGE_ID)
                .setTopic(TOPIC)
                .setPayload(com.google.protobuf.ByteString.copyFrom(PAYLOAD))
                .putMetadata("key", "val")
                .setRetryCount(2)
                .build();
    }

    @BeforeEach
    void setUp() throws IOException {
        fakeService = new FakeService(List.of(sampleResponse()));

        server = InProcessServerBuilder.forName(SERVER_NAME)
                .directExecutor()
                .addService(fakeService)
                .build()
                .start();

        final var channel = InProcessChannelBuilder.forName(SERVER_NAME)
                .directExecutor()
                .build();

        client = QueueTiClientTestFixture.newClient(channel);

        context = new GenericApplicationContext(new DefaultListableBeanFactory());
        context.refresh();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (adapter != null && adapter.isRunning()) adapter.stop();
        context.close();
        client.close();
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
    }

    private QueueTiInboundChannelAdapter buildAdapter(
            final QueueTiInboundChannelAdapter.AcknowledgeMode mode) {
        adapter = new QueueTiInboundChannelAdapter(client, TOPIC);
        adapter.setAcknowledgeMode(mode);
        adapter.setBeanFactory(context.getBeanFactory());
        return adapter;
    }

    // =========================================================================
    // AUTO mode tests
    // =========================================================================

    @Test
    @DisplayName("AUTO — received message has correct payload and headers")
    void auto_messagePayloadAndHeaders() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.AUTO);
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        final Message<?> msg = output.receive(5_000);

        assertThat(msg).isNotNull();
        assertThat((byte[]) msg.getPayload()).isEqualTo(PAYLOAD);
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.MESSAGE_ID)).isEqualTo(MESSAGE_ID);
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.TOPIC)).isEqualTo(TOPIC);
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.RETRY_COUNT)).isEqualTo(2);
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.METADATA))
                .isEqualTo(Map.of("key", "val"));
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.CREATED_AT))
                .isInstanceOf(Instant.class);
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.ACKNOWLEDGMENT)).isNull();
    }

    @Test
    @DisplayName("AUTO — successful processing sends AckRequest to server")
    void auto_successfulProcessingSendsAck() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.AUTO);
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        output.receive(5_000);
        final boolean acked = fakeService.ackLatch.await(5, TimeUnit.SECONDS);

        assertThat(acked).isTrue();
        assertThat(fakeService.capturedAcks).hasSize(1);
        assertThat(fakeService.capturedAcks.get(0).getId()).isEqualTo(MESSAGE_ID);
    }

    @Test
    @DisplayName("AUTO — downstream exception sends NackRequest to server")
    void auto_downstreamExceptionSendsNack() throws InterruptedException {
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.AUTO);
        adapter.setOutputChannel(new DirectChannel() {
            @Override
            protected boolean doSend(final Message<?> message, final long timeout) {
                throw new RuntimeException("downstream failure");
            }
        });
        adapter.afterPropertiesSet();
        adapter.start();

        final boolean nacked = fakeService.nackLatch.await(5, TimeUnit.SECONDS);

        assertThat(nacked).isTrue();
        assertThat(fakeService.capturedNacks).hasSize(1);
        assertThat(fakeService.capturedNacks.get(0).getId()).isEqualTo(MESSAGE_ID);
    }

    // =========================================================================
    // MANUAL mode tests
    // =========================================================================

    @Test
    @DisplayName("MANUAL — received message has ACKNOWLEDGMENT header, no ack sent yet")
    void manual_messageHasAcknowledgmentHeader() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.MANUAL);
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        final Message<?> msg = output.receive(5_000);

        assertThat(msg).isNotNull();
        assertThat(msg.getHeaders().get(QueueTiMessageHeaders.ACKNOWLEDGMENT))
                .isInstanceOf(QueueTiAcknowledgment.class);
        // No ack yet — not settled
        assertThat(fakeService.capturedAcks).isEmpty();

        // Settle to unblock the handler thread
        final QueueTiAcknowledgment ack =
                (QueueTiAcknowledgment) msg.getHeaders().get(QueueTiMessageHeaders.ACKNOWLEDGMENT);
        ack.acknowledge();
        fakeService.ackLatch.await(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("MANUAL — ack.acknowledge() sends AckRequest to server")
    void manual_acknowledgeCallSendsAck() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.MANUAL);
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        final Message<?> msg = output.receive(5_000);
        assertThat(msg).isNotNull();

        final QueueTiAcknowledgment ack =
                (QueueTiAcknowledgment) msg.getHeaders().get(QueueTiMessageHeaders.ACKNOWLEDGMENT);
        ack.acknowledge();

        final boolean acked = fakeService.ackLatch.await(5, TimeUnit.SECONDS);

        assertThat(acked).isTrue();
        assertThat(fakeService.capturedAcks).hasSize(1);
        assertThat(fakeService.capturedAcks.get(0).getId()).isEqualTo(MESSAGE_ID);
    }

    @Test
    @DisplayName("MANUAL — ack.nack(reason) sends NackRequest with reason to server")
    void manual_nackCallSendsNack() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.MANUAL);
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        final Message<?> msg = output.receive(5_000);
        assertThat(msg).isNotNull();

        final QueueTiAcknowledgment ack =
                (QueueTiAcknowledgment) msg.getHeaders().get(QueueTiMessageHeaders.ACKNOWLEDGMENT);
        ack.nack("processing failed");

        final boolean nacked = fakeService.nackLatch.await(5, TimeUnit.SECONDS);

        assertThat(nacked).isTrue();
        assertThat(fakeService.capturedNacks).hasSize(1);
        assertThat(fakeService.capturedNacks.get(0).getId()).isEqualTo(MESSAGE_ID);
        assertThat(fakeService.capturedNacks.get(0).getError()).isEqualTo("processing failed");
    }

    @Test
    @DisplayName("MANUAL — settlement timeout sends NackRequest after timeout")
    void manual_settlementTimeoutSendsNack() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.MANUAL);
        adapter.setSettlementTimeout(Duration.ofMillis(200));
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        final Message<?> msg = output.receive(5_000);
        assertThat(msg).isNotNull();

        // Intentionally do NOT call ack/nack — let the timeout expire

        final boolean nacked = fakeService.nackLatch.await(5, TimeUnit.SECONDS);

        assertThat(nacked).isTrue();
        assertThat(fakeService.capturedNacks).hasSize(1);
        assertThat(fakeService.capturedNacks.get(0).getId()).isEqualTo(MESSAGE_ID);
    }

    // =========================================================================
    // Lifecycle test
    // =========================================================================

    @Test
    @DisplayName("Lifecycle — stop() interrupts consumer thread without hanging")
    void lifecycle_stopDoesNotHang() throws InterruptedException {
        final QueueChannel output = new QueueChannel();
        final QueueTiInboundChannelAdapter adapter =
                buildAdapter(QueueTiInboundChannelAdapter.AcknowledgeMode.AUTO);
        adapter.setOutputChannel(output);
        adapter.afterPropertiesSet();
        adapter.start();

        assertThat(adapter.isRunning()).isTrue();

        adapter.stop();

        assertThat(adapter.isRunning()).isFalse();
    }
}
