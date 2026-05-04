package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.AckRequest;
import de.joesst.dev.queueti.pb.AckResponse;
import de.joesst.dev.queueti.pb.BatchDequeueRequest;
import de.joesst.dev.queueti.pb.BatchDequeueResponse;
import de.joesst.dev.queueti.pb.DequeueResponse;
import de.joesst.dev.queueti.pb.NackRequest;
import de.joesst.dev.queueti.pb.NackResponse;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * BDD-style integration tests for {@link Consumer#consumeBatch(int, BatchMessageHandler)}
 * using an in-process gRPC server.
 */
class ConsumerBatchTest {

    // =========================================================================
    // Fake gRPC service
    // =========================================================================

    /**
     * Configurable in-process batch service. Each call to {@code batchDequeue()} pops one
     * {@link BatchDequeueResponse} from the queue; when the queue is empty, returns an
     * empty {@link BatchDequeueResponse}.
     *
     * <p>Ack/nack requests are captured and signal their respective latches.
     */
    private static class FakeBatchService extends QueueServiceGrpc.QueueServiceImplBase {

        /** Pre-loaded responses; each poll call consumes one entry. */
        final Queue<BatchDequeueResponse> responses = new ConcurrentLinkedQueue<>();

        /** Captures the last {@code BatchDequeueRequest} received. */
        volatile BatchDequeueRequest lastRequest;

        /**
         * Records the nanosecond timestamp of every {@code batchDequeue} call so tests
         * can measure inter-call timing.
         */
        volatile long lastCallNanos = 0;

        /** Signalled on every received ack. */
        volatile CountDownLatch ackLatch = new CountDownLatch(1);
        /** All ack requests captured in order. */
        final List<AckRequest> capturedAcks = new ArrayList<>();

        /** Signalled on every received nack. */
        volatile CountDownLatch nackLatch = new CountDownLatch(1);
        /** All nack requests captured in order. */
        final List<NackRequest> capturedNacks = new ArrayList<>();

        @Override
        public synchronized void batchDequeue(
                final BatchDequeueRequest request,
                final StreamObserver<BatchDequeueResponse> responseObserver) {
            lastRequest = request;
            lastCallNanos = System.nanoTime();
            final BatchDequeueResponse next = responses.poll();
            responseObserver.onNext(next != null ? next : BatchDequeueResponse.getDefaultInstance());
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

    // =========================================================================
    // Infrastructure
    // =========================================================================

    private FakeBatchService fakeService;
    private Server inProcessServer;
    private QueueServiceGrpc.QueueServiceStub asyncStub;
    private QueueServiceGrpc.QueueServiceFutureStub futureStub;

    @BeforeEach
    void setUp() throws IOException {
        fakeService = new FakeBatchService();
        final var serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(fakeService)
                .build()
                .start();
        // Do NOT use directExecutor() on the channel — see ConsumerStreamingTest for rationale.
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

    /** Builds a minimal {@link DequeueResponse} with the given id. */
    private static DequeueResponse dequeueResponse(final String id) {
        return DequeueResponse.newBuilder()
                .setId(id)
                .setTopic("test-topic")
                .build();
    }

    /** Wraps one or more {@link DequeueResponse}s in a {@link BatchDequeueResponse}. */
    private static BatchDequeueResponse batchOf(final DequeueResponse... msgs) {
        return BatchDequeueResponse.newBuilder()
                .addAllMessages(List.of(msgs))
                .build();
    }

    /**
     * Starts {@link Consumer#consumeBatch(int, BatchMessageHandler)} on a virtual thread
     * and returns that thread so the caller can interrupt it when done.
     */
    private Thread startBatchConsuming(
            final Consumer consumer,
            final int batchSize,
            final BatchMessageHandler handler) {
        return Thread.ofVirtual().start(() -> consumer.consumeBatch(batchSize, handler));
    }

    /**
     * Starts {@link Consumer#consumeBatch(int, BatchMessageHandler, BatchOptions)} on a
     * virtual thread and returns that thread so the caller can interrupt it when done.
     */
    private Thread startBatchConsuming(
            final Consumer consumer,
            final int batchSize,
            final BatchMessageHandler handler,
            final BatchOptions batchOptions) {
        return Thread.ofVirtual().start(() -> consumer.consumeBatch(batchSize, handler, batchOptions));
    }

    // =========================================================================
    // Tests
    // =========================================================================

    @Test
    @DisplayName("delivers batch messages to handler and acks each one")
    void consumeBatch_returns_messages_to_handler() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(dequeueResponse("msg-1"), dequeueResponse("msg-2")));
        fakeService.ackLatch = new CountDownLatch(2);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 2, messages -> {
            for (final Message msg : messages) {
                msg.ack();
            }
            return null;
        });

        try {
            // Then
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS))
                    .as("both acks should be received within 2s")
                    .isTrue();
            assertThat(fakeService.capturedAcks).hasSize(2);
            final var ackedIds = fakeService.capturedAcks.stream()
                    .map(AckRequest::getId)
                    .toList();
            assertThat(ackedIds).containsExactlyInAnyOrder("msg-1", "msg-2");
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("backs off when server returns empty batch")
    void consumeBatch_backs_off_on_empty_response() throws InterruptedException, IOException {
        // Given — first response is explicitly empty; second contains one message.
        // The consumer must sleep BACKOFF_START (500ms) between the two calls.
        // We capture the nanosecond timestamp of the first (empty) call via a latch, then
        // compare it to the timestamp of the second (non-empty) call stored in lastCallNanos.
        final CountDownLatch firstCallLatch = new CountDownLatch(1);
        final long[] firstCallNanos = {0};

        final FakeBatchService timedFake = new FakeBatchService() {
            private int callCount = 0;
            @Override
            public synchronized void batchDequeue(
                    final BatchDequeueRequest request,
                    final io.grpc.stub.StreamObserver<BatchDequeueResponse> responseObserver) {
                callCount++;
                if (callCount == 1) {
                    firstCallNanos[0] = System.nanoTime();
                    firstCallLatch.countDown();
                    responseObserver.onNext(BatchDequeueResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                } else {
                    lastCallNanos = System.nanoTime();
                    lastRequest = request;
                    responseObserver.onNext(batchOf(dequeueResponse("msg-backoff")));
                    responseObserver.onCompleted();
                }
            }
        };

        // Rebuild the in-process server using the timing-aware fake.
        inProcessServer.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        final var serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(timedFake)
                .build()
                .start();
        final var channel = InProcessChannelBuilder.forName(serverName).build();
        final var localFutureStub = QueueServiceGrpc.newFutureStub(channel);
        final var localAsyncStub  = QueueServiceGrpc.newStub(channel);
        timedFake.ackLatch = new CountDownLatch(1);

        final var consumer = new Consumer(localAsyncStub, localFutureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null);

        try {
            // Then
            assertThat(firstCallLatch.await(2, TimeUnit.SECONDS))
                    .as("first (empty) call should arrive within 2s")
                    .isTrue();
            assertThat(timedFake.ackLatch.await(3, TimeUnit.SECONDS))
                    .as("message from second poll should be acked within 3s")
                    .isTrue();

            final long gapMs = (timedFake.lastCallNanos - firstCallNanos[0]) / 1_000_000L;
            assertThat(gapMs)
                    .as("gap between empty-response call and next poll should be >= 500ms")
                    .isGreaterThanOrEqualTo(500L);
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("resets backoff to starting value after receiving a non-empty batch")
    void consumeBatch_resets_backoff_on_non_empty_response() throws InterruptedException {
        // Given — sequence: empty, empty, one-message, <more empties via exhausted queue>.
        // After two empty batches the backoff doubles to 1000ms; after the non-empty batch
        // it resets to 500ms. We verify the poll *following* the non-empty batch arrives
        // within a generous 1500ms window (not at the doubled ~2000ms schedule).
        fakeService.responses.add(BatchDequeueResponse.getDefaultInstance()); // empty  → backoff 500ms
        fakeService.responses.add(BatchDequeueResponse.getDefaultInstance()); // empty  → backoff 1000ms
        fakeService.responses.add(batchOf(dequeueResponse("msg-reset")));     // non-empty → reset to 500ms
        // 4th+ calls return empty (queue exhausted); we observe their timing.
        fakeService.ackLatch = new CountDownLatch(1);

        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null);

        try {
            // Then — wait for the non-empty message's ack, establishing a timing baseline.
            assertThat(fakeService.ackLatch.await(5, TimeUnit.SECONDS))
                    .as("message should be acked within 5s")
                    .isTrue();

            final long afterNonEmptyNanos = fakeService.lastCallNanos;

            // Give the consumer time to make the next (empty) call at the reset backoff of 500ms.
            // We allow 1500ms — well inside a reset schedule but safely below the doubled 2000ms.
            Thread.sleep(1500);

            assertThat(fakeService.lastCallNanos)
                    .as("a new poll should have occurred within 1500ms of the non-empty batch (backoff reset)")
                    .isGreaterThan(afterNonEmptyNanos);
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("forwards configured consumer group in BatchDequeueRequest")
    void consumeBatch_forwards_consumer_group_in_request() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(dequeueResponse("msg-cg")));
        fakeService.ackLatch = new CountDownLatch(1);
        final var options = ConsumerOptions.builder().consumerGroup("g1").build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null);

        try {
            // Then
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fakeService.lastRequest.getConsumerGroup()).isEqualTo("g1");
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("sends empty string as consumer group when using default options")
    void consumeBatch_forwards_empty_consumer_group_by_default() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(dequeueResponse("msg-default-cg")));
        fakeService.ackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null);

        try {
            // Then
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fakeService.lastRequest.getConsumerGroup()).isEqualTo("");
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("sends requested batch size as count in BatchDequeueRequest")
    void consumeBatch_sends_batch_size_in_request() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(dequeueResponse("msg-count")));
        fakeService.ackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 5, messages -> null);

        try {
            // Then
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fakeService.lastRequest.getCount()).isEqualTo(5);
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("throws IllegalArgumentException immediately when batchSize is zero")
    void consumeBatch_throws_immediately_for_zero_batch_size() {
        // Given
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When / Then
        assertThatThrownBy(() -> consumer.consumeBatch(0, messages -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchSize must be >= 1");
        // No RPC should have been made
        assertThat(fakeService.lastRequest).isNull();
    }

    @Test
    @DisplayName("nacks all messages in batch when handler throws")
    void consumeBatch_handler_exception_triggers_nack_on_all_messages() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(
                dequeueResponse("msg-nack-1"), dequeueResponse("msg-nack-2")));
        fakeService.nackLatch = new CountDownLatch(2);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 2, messages -> {
            throw new RuntimeException("handler-error");
        });

        try {
            // Then
            assertThat(fakeService.nackLatch.await(2, TimeUnit.SECONDS))
                    .as("both nacks should be received within 2s")
                    .isTrue();
            assertThat(fakeService.capturedNacks).hasSize(2);
            final var nackedIds = fakeService.capturedNacks.stream()
                    .map(NackRequest::getId)
                    .toList();
            assertThat(nackedIds).containsExactlyInAnyOrder("msg-nack-1", "msg-nack-2");
            assertThat(fakeService.capturedNacks)
                    .allMatch(r -> "handler-error".equals(r.getError()));
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("includes visibility timeout in request when configured")
    void consumeBatch_sends_visibility_timeout_when_set() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(dequeueResponse("msg-vt")));
        fakeService.ackLatch = new CountDownLatch(1);
        final var options = ConsumerOptions.builder().visibilityTimeoutSeconds(30).build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", options);

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null);

        try {
            // Then
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fakeService.lastRequest.hasVisibilityTimeoutSeconds()).isTrue();
            assertThat(fakeService.lastRequest.getVisibilityTimeoutSeconds()).isEqualTo(30);
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("omits visibility timeout from request when not configured")
    void consumeBatch_does_not_send_visibility_timeout_when_not_set() throws InterruptedException {
        // Given
        fakeService.responses.add(batchOf(dequeueResponse("msg-no-vt")));
        fakeService.ackLatch = new CountDownLatch(1);
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null);

        try {
            // Then
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fakeService.lastRequest.hasVisibilityTimeoutSeconds()).isFalse();
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("BatchOptions consumer group overrides ConsumerOptions consumer group")
    void consumeBatch_with_batch_options_uses_batch_options_consumer_group() throws InterruptedException {
        // Given — consumer built with "from-consumer", call made with BatchOptions "from-batch"
        fakeService.responses.add(batchOf(dequeueResponse("msg-bo")));
        fakeService.ackLatch = new CountDownLatch(1);
        final var consumerOptions = ConsumerOptions.builder().consumerGroup("from-consumer").build();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic", consumerOptions);
        final var batchOptions = BatchOptions.builder().consumerGroup("from-batch").build();

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> null, batchOptions);

        try {
            // Then — the request must carry "from-batch", not "from-consumer"
            assertThat(fakeService.ackLatch.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fakeService.lastRequest.getConsumerGroup()).isEqualTo("from-batch");
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }

    @Test
    @DisplayName("maps missing created_at field to Instant.EPOCH")
    void consumeBatch_null_created_at_becomes_epoch() throws InterruptedException {
        // Given — DequeueResponse without created_at set
        final var raw = DequeueResponse.newBuilder()
                .setId("msg-epoch")
                .setTopic("test-topic")
                .build(); // no created_at
        fakeService.responses.add(batchOf(raw));

        final CountDownLatch handlerLatch = new CountDownLatch(1);
        final AtomicReference<Message> capturedMessage = new AtomicReference<>();
        final var consumer = new Consumer(asyncStub, futureStub, "test-topic",
                ConsumerOptions.builder().build());

        // When
        final var thread = startBatchConsuming(consumer, 1, messages -> {
            capturedMessage.set(messages.get(0));
            handlerLatch.countDown();
            return null;
        });

        try {
            // Then
            assertThat(handlerLatch.await(2, TimeUnit.SECONDS))
                    .as("handler should be called within 2s")
                    .isTrue();
            assertThat(capturedMessage.get().createdAt())
                    .as("missing created_at should map to Instant.EPOCH")
                    .isEqualTo(Instant.EPOCH);
        } finally {
            thread.interrupt();
            thread.join(1000);
        }
    }
}
