package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.AckRequest;
import de.joesst.dev.queueti.pb.AckResponse;
import de.joesst.dev.queueti.pb.NackRequest;
import de.joesst.dev.queueti.pb.NackResponse;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import de.joesst.dev.queueti.pb.SubscribeRequest;
import de.joesst.dev.queueti.pb.SubscribeResponse;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * BDD-style integration tests for {@link Consumer} streaming behaviour using an
 * in-process gRPC server.
 */
class ConsumerStreamingTest {

    // =========================================================================
    // Fake gRPC service
    // =========================================================================

    /**
     * Configurable in-process service. On each {@code subscribe()} call it emits
     * all queued {@link SubscribeResponse}s and then calls {@code onCompleted()} (or
     * {@code onError} on the first call if {@link #errorOnFirstCall} is {@code true}).
     *
     * <p>Ack/nack RPCs are captured and signal their respective latches.
     */
    private static class FakeSubscribeService
            extends QueueServiceGrpc.QueueServiceImplBase {

        /** Responses to emit on subscribe. Replenished between calls by tests. */
        volatile Queue<SubscribeResponse> responses = new ConcurrentLinkedQueue<>();

        /** When {@code true}, the first subscribe call fires onError instead of emitting. */
        volatile boolean errorOnFirstCall = false;
        private volatile boolean firstCallDone = false;

        /** Set by tests; each countdown corresponds to one ack received. */
        volatile CountDownLatch ackLatch = new CountDownLatch(1);
        /** All ack requests captured in order. */
        final List<AckRequest> capturedAcks = new ArrayList<>();

        /** Set by tests; each countdown corresponds to one nack received. */
        volatile CountDownLatch nackLatch = new CountDownLatch(1);
        /** All nack requests captured in order. */
        final List<NackRequest> capturedNacks = new ArrayList<>();

        /** Captures all subscribe requests in order. */
        final List<SubscribeRequest> capturedSubscribeRequests = new ArrayList<>();

        @Override
        public synchronized void subscribe(
                final SubscribeRequest request,
                final StreamObserver<SubscribeResponse> responseObserver) {
            capturedSubscribeRequests.add(request);
            if (errorOnFirstCall && !firstCallDone) {
                firstCallDone = true;
                responseObserver.onError(Status.INTERNAL.asException());
                return;
            }
            firstCallDone = true;
            final Queue<SubscribeResponse> toEmit = responses;
            SubscribeResponse next;
            while ((next = toEmit.poll()) != null) {
                responseObserver.onNext(next);
            }
            responseObserver.onCompleted();
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

    /**
     * Service variant for reconnect-after-complete tests: the first {@code subscribe()}
     * call immediately fires {@code onCompleted()} with no messages; the second call emits
     * {@code secondCallMessageId} and then completes. Ack/nack tracking is inherited from
     * {@link FakeSubscribeService}.
     */
    private static final class TwoCallSubscribeService extends FakeSubscribeService {

        private final String secondCallMessageId;
        private int callCount = 0;

        TwoCallSubscribeService(final String secondCallMessageId) {
            this.secondCallMessageId = secondCallMessageId;
        }

        @Override
        public synchronized void subscribe(
                final SubscribeRequest request,
                final StreamObserver<SubscribeResponse> responseObserver) {
            callCount++;
            if (callCount == 1) {
                responseObserver.onCompleted();
            } else {
                responseObserver.onNext(response(secondCallMessageId));
                responseObserver.onCompleted();
            }
        }
    }

    // =========================================================================
    // Infrastructure
    // =========================================================================

    private FakeSubscribeService fakeService;
    private Server inProcessServer;
    private QueueServiceGrpc.QueueServiceStub asyncStub;
    private QueueServiceGrpc.QueueServiceFutureStub futureStub;

    @BeforeEach
    void setUp() throws IOException {
        fakeService = new FakeSubscribeService();
        final var serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(fakeService)
                .build()
                .start();
        // Do NOT use directExecutor() on the channel: with directExecutor the gRPC
        // client callbacks run on the same thread as the server's onNext() loop.
        // That causes a deadlock when Consumer.onNext() calls sem.acquire() with
        // concurrency=1 — the server thread blocks, never finishing the response loop,
        // so the semaphore is never released.  Using the default executor gives each
        // side its own thread pool and lets the semaphore logic work correctly.
        final var channel = InProcessChannelBuilder
                .forName(serverName)
                .build();
        asyncStub  = QueueServiceGrpc.newStub(channel);
        futureStub = QueueServiceGrpc.newFutureStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        inProcessServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Builds a minimal {@link SubscribeResponse} with the given id. */
    private static SubscribeResponse response(final String id) {
        return SubscribeResponse.newBuilder().setId(id).setTopic("test-topic").build();
    }

    /**
     * Starts {@code consume()} on a fresh virtual thread and returns that thread so the
     * caller can interrupt it when done.
     */
    private Thread startConsuming(final Consumer consumer, final MessageHandler handler) {
        return Thread.ofVirtual().start(() -> consumer.consume(handler));
    }

    // =========================================================================
    // Tests
    // =========================================================================

    @Test
    @DisplayName("acks message when handler returns normally")
    void consume_acks_message_when_handler_returns_null() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-1"));
        fakeService.ackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startConsuming(consumer, msg -> null);

        // Then
        assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS))
                .as("ack should be received within 2s")
                .isTrue();
        assertThat(fakeService.capturedAcks).hasSize(1);
        assertThat(fakeService.capturedAcks.get(0).getId()).isEqualTo("msg-1");
        assertThat(fakeService.capturedAcks.get(0).getConsumerGroup()).isEqualTo("");
        thread.interrupt();
    }

    @Test
    @DisplayName("nacks message when handler throws a RuntimeException")
    void consume_nacks_message_when_handler_throws() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-2"));
        fakeService.nackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startConsuming(consumer, msg -> {
            throw new RuntimeException("oops");
        });

        // Then
        assertThat(fakeService.nackLatch.await(2, TimeUnit.SECONDS))
                .as("nack should be received within 2s")
                .isTrue();
        assertThat(fakeService.capturedNacks).hasSize(1);
        assertThat(fakeService.capturedNacks.get(0).getId()).isEqualTo("msg-2");
        assertThat(fakeService.capturedNacks.get(0).getError()).isEqualTo("oops");
        thread.interrupt();
    }

    @Test
    @DisplayName("nacks message when handler throws an unchecked Error")
    void consume_nacks_message_on_unchecked_error() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-3"));
        fakeService.nackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startConsuming(consumer, msg -> {
            throw new OutOfMemoryError("simulated OOM");
        });

        // Then
        assertThat(fakeService.nackLatch.await(2, TimeUnit.SECONDS))
                .as("nack should be received within 2s even for Errors")
                .isTrue();
        assertThat(fakeService.capturedNacks).hasSize(1);
        thread.interrupt();
    }

    @Test
    @DisplayName("forwards consumer group on ack when configured")
    void consume_forwards_consumer_group_on_ack() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-4"));
        fakeService.ackLatch = new CountDownLatch(1);
        final var options = ConsumerOptions.builder().consumerGroup("cg-1").build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        // When
        final var thread = startConsuming(consumer, msg -> null);

        // Then
        assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(fakeService.capturedAcks.get(0).getConsumerGroup()).isEqualTo("cg-1");
        thread.interrupt();
    }

    @Test
    @DisplayName("forwards consumer group on nack when configured")
    void consume_forwards_consumer_group_on_nack() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-5"));
        fakeService.nackLatch = new CountDownLatch(1);
        final var options = ConsumerOptions.builder().consumerGroup("cg-1").build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        // When
        final var thread = startConsuming(consumer, msg -> {
            throw new RuntimeException("failure");
        });

        // Then
        assertThat(fakeService.nackLatch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(fakeService.capturedNacks.get(0).getConsumerGroup()).isEqualTo("cg-1");
        thread.interrupt();
    }

    @Test
    @DisplayName("processes multiple messages concurrently up to concurrency limit")
    void consume_processes_multiple_messages_concurrently() throws InterruptedException {
        // Given — 4 messages, concurrency 4; all 4 handlers wait at a barrier so they
        // must all be in-flight simultaneously for the test to proceed.
        final int count = 4;
        final CyclicBarrier barrier = new CyclicBarrier(count);
        fakeService.ackLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            fakeService.responses.add(response("msg-c-" + i));
        }
        final var options = ConsumerOptions.builder().concurrency(count).build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        // When
        final var thread = startConsuming(consumer, msg -> {
            try {
                barrier.await(3, TimeUnit.SECONDS);
            } catch (BrokenBarrierException | java.util.concurrent.TimeoutException e) {
                throw new RuntimeException("barrier not reached by all handlers", e);
            }
            return null;
        });

        // Then — all 4 acks arrive, proving all 4 ran simultaneously
        assertThat(fakeService.ackLatch.await(5, TimeUnit.SECONDS))
                .as("all 4 messages should be acked within 5s")
                .isTrue();
        assertThat(fakeService.capturedAcks).hasSize(count);
        thread.interrupt();
    }

    @Test
    @DisplayName("respects concurrency limit of 1 by never processing more than one message at a time")
    void consume_respects_concurrency_limit() throws InterruptedException {
        // Given — 3 messages, concurrency 1; track in-flight count with an AtomicInteger
        final int count = 3;
        fakeService.ackLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            fakeService.responses.add(response("msg-s-" + i));
        }
        final var options = ConsumerOptions.builder().concurrency(1).build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        final AtomicInteger inFlight = new AtomicInteger(0);
        final AtomicInteger maxObservedInFlight = new AtomicInteger(0);

        // When
        final var thread = startConsuming(consumer, msg -> {
            final int current = inFlight.incrementAndGet();
            maxObservedInFlight.accumulateAndGet(current, Math::max);
            // Small yield to give scheduler a chance to violate the limit if buggy.
            Thread.sleep(10);
            inFlight.decrementAndGet();
            return null;
        });

        // Then
        assertThat(fakeService.ackLatch.await(5, TimeUnit.SECONDS))
                .as("all 3 messages should be processed within 5s")
                .isTrue();
        assertThat(maxObservedInFlight.get())
                .as("max in-flight should never exceed 1")
                .isLessThanOrEqualTo(1);
        thread.interrupt();
    }

    @Test
    @DisplayName("reconnects after a stream error and processes next message")
    void consume_reconnects_after_stream_error() throws InterruptedException {
        // Given — first subscribe fires onError; second call emits one message
        fakeService.errorOnFirstCall = true;
        fakeService.responses.add(response("msg-reconnect"));
        fakeService.ackLatch = new CountDownLatch(1);

        // Override backoff to avoid a 500ms wait in the test.
        // We cannot inject backoff directly, so we rely on the fast in-process server
        // and simply give 3s total for the test to complete.
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startConsuming(consumer, msg -> null);

        // Then — the message from the second stream connection should be acked
        assertThat(fakeService.ackLatch.await(3, TimeUnit.SECONDS))
                .as("message should be acked after reconnect")
                .isTrue();
        assertThat(fakeService.capturedAcks.get(0).getId()).isEqualTo("msg-reconnect");
        thread.interrupt();
    }

    @Test
    @DisplayName("reconnects immediately after a clean server-side stream close")
    void consume_reconnects_after_stream_completed() throws InterruptedException, IOException {
        // Given — first subscribe fires onCompleted() with no messages;
        // second subscribe emits one message. A call-count-aware subclass drives this.
        final var svc = new TwoCallSubscribeService("msg-after-complete");
        inProcessServer.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        final var serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(svc)
                .build()
                .start();
        final var channel = InProcessChannelBuilder.forName(serverName).build();
        asyncStub  = QueueServiceGrpc.newStub(channel);
        futureStub = QueueServiceGrpc.newFutureStub(channel);

        svc.ackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startConsuming(consumer, msg -> null);

        // Then
        assertThat(svc.ackLatch.await(3, TimeUnit.SECONDS))
                .as("message from the second stream should be acked within 3s")
                .isTrue();
        assertThat(svc.capturedAcks.get(0).getId()).isEqualTo("msg-after-complete");
        thread.interrupt();
    }

    @Test
    @DisplayName("sends consumer group in SubscribeRequest when configured")
    void consume_sends_consumer_group_in_subscribe_request() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-cg"));
        fakeService.ackLatch = new CountDownLatch(1);
        final var options = ConsumerOptions.builder().consumerGroup("my-group").build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        // When
        final var thread = startConsuming(consumer, msg -> null);

        // Then
        assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(fakeService.capturedSubscribeRequests).isNotEmpty();
        assertThat(fakeService.capturedSubscribeRequests.get(0).getConsumerGroup())
                .isEqualTo("my-group");
        thread.interrupt();
    }

    @Test
    @DisplayName("sends empty consumer group in SubscribeRequest when using default options")
    void consume_sends_empty_consumer_group_by_default() throws InterruptedException {
        // Given
        fakeService.responses.add(response("msg-default-cg"));
        fakeService.ackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startConsuming(consumer, msg -> null);

        // Then
        assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(fakeService.capturedSubscribeRequests).isNotEmpty();
        assertThat(fakeService.capturedSubscribeRequests.get(0).getConsumerGroup()).isEqualTo("");
        thread.interrupt();
    }
}
