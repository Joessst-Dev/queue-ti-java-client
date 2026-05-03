package de.joesst.dev.queueti;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import de.joesst.dev.queueti.pb.AckRequest;
import de.joesst.dev.queueti.pb.AckResponse;
import de.joesst.dev.queueti.pb.NackRequest;
import de.joesst.dev.queueti.pb.NackResponse;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import de.joesst.dev.queueti.pb.SubscribeRequest;
import de.joesst.dev.queueti.pb.SubscribeResponse;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Consumes messages from a queue-ti topic via a persistent server-streaming subscription.
 *
 * <p>Obtain an instance via {@link QueueTiClient#newConsumer(String)} or
 * {@link QueueTiClient#newConsumer(String, ConsumerOptions)}.
 *
 * <p>Call {@link #consume(MessageHandler)} to start the subscription loop; the method
 * blocks until the calling thread is interrupted. The consumer reconnects automatically
 * after any stream error or clean server-initiated close, using exponential backoff
 * starting at {@value #BACKOFF_START_MS} ms and capped at
 * {@value #BACKOFF_MAX_SECS} s.
 *
 * <p>Thread-safe: {@code consume} is designed to be called from a single dedicated
 * thread. The ack/nack futures it produces may be observed from any thread.
 */
public final class Consumer {

    private static final long BACKOFF_START_MS = 500L;
    private static final long BACKOFF_MAX_SECS = 30L;

    /** Exponential-backoff starting duration. */
    static final Duration BACKOFF_START = Duration.ofMillis(BACKOFF_START_MS);
    /** Exponential-backoff ceiling. */
    static final Duration BACKOFF_MAX   = Duration.ofSeconds(BACKOFF_MAX_SECS);

    private static final Logger logger = Logger.getLogger(Consumer.class.getName());

    private final QueueServiceGrpc.QueueServiceStub asyncStub;
    private final QueueServiceGrpc.QueueServiceFutureStub futureStub;
    private final String topic;
    private final ConsumerOptions options;

    /**
     * Package-private constructor — callers must use
     * {@link QueueTiClient#newConsumer(String)} or
     * {@link QueueTiClient#newConsumer(String, ConsumerOptions)}.
     *
     * @param asyncStub  async stub for server-streaming subscribe calls
     * @param futureStub future stub for unary ack/nack calls
     * @param topic      topic to consume from; must not be {@code null}
     * @param options    consumer configuration; must not be {@code null}
     */
    Consumer(
            final QueueServiceGrpc.QueueServiceStub asyncStub,
            final QueueServiceGrpc.QueueServiceFutureStub futureStub,
            final String topic,
            final ConsumerOptions options) {
        this.asyncStub = asyncStub;
        this.futureStub = futureStub;
        this.topic = topic;
        this.options = options;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Starts the streaming consumer loop, delivering each message to {@code handler}.
     *
     * <p>This method blocks until the calling thread is interrupted. On interrupt, in-flight
     * message dispatches are given up to 5 seconds to complete before the underlying executor
     * is abandoned.
     *
     * <p>Each message is dispatched on a virtual-thread executor. The {@code handler} is called
     * with the message; if it returns normally (even {@code null}) the message is acked; if it
     * throws any {@link Throwable} the message is nacked with the throwable's message.
     *
     * <p>Concurrency is bounded by {@link ConsumerOptions#getConcurrency()}: at most that many
     * messages are dispatched simultaneously. Additional messages block the gRPC callback thread
     * until a slot becomes available.
     *
     * @param handler the callback invoked for every delivered message; must not be {@code null}
     */
    public void consume(final MessageHandler handler) {
        // Mutable state visible to inner-class lambdas via one-element arrays.
        // We use single-element arrays because lambdas cannot close over non-final locals.
        final boolean[] cancelledRef = {false};
        final Semaphore sem = new Semaphore(options.getConcurrency());
        final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofVirtual().name("queue-ti-reconnect-scheduler").unstarted(r));
        final CountDownLatch doneLatch = new CountDownLatch(1);
        final Duration[] backoff = {BACKOFF_START};

        // Kick off the first stream connection immediately.
        scheduleConnect(Duration.ZERO, cancelledRef, sem, executor, scheduler,
                        doneLatch, backoff, handler);

        try {
            doneLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancelledRef[0] = true;
        }

        cancelledRef[0] = true;
        scheduler.shutdownNow();
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stub for the batch-consume mode.
     *
     * @param batchSize maximum number of messages to deliver per batch
     * @param handler   handler invoked with each batch
     * @throws UnsupportedOperationException always — not yet implemented
     */
    public void consumeBatch(final int batchSize, final BatchMessageHandler handler) {
        // TODO: Phase 10
        throw new UnsupportedOperationException("consumeBatch is not yet implemented (Phase 10)");
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Schedules a call to {@link #openStream} after {@code delay}.
     */
    private void scheduleConnect(
            final Duration delay,
            final boolean[] cancelledRef,
            final Semaphore sem,
            final ExecutorService executor,
            final ScheduledExecutorService scheduler,
            final CountDownLatch doneLatch,
            final Duration[] backoff,
            final MessageHandler handler) {
        final long delayMs = delay.toMillis();
        scheduler.schedule(
                () -> openStream(cancelledRef, sem, executor, scheduler,
                                 doneLatch, backoff, handler),
                delayMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Opens one server-streaming Subscribe call and wires a {@link StreamObserver} that
     * handles reconnection on error or clean close.
     */
    private void openStream(
            final boolean[] cancelledRef,
            final Semaphore sem,
            final ExecutorService executor,
            final ScheduledExecutorService scheduler,
            final CountDownLatch doneLatch,
            final Duration[] backoff,
            final MessageHandler handler) {

        if (cancelledRef[0]) {
            return;
        }

        final SubscribeRequest.Builder req = SubscribeRequest.newBuilder()
                .setTopic(topic)
                .setConsumerGroup(options.getConsumerGroup());
        if (options.getVisibilityTimeoutSeconds() != null) {
            req.setVisibilityTimeoutSeconds(options.getVisibilityTimeoutSeconds());
        }

        asyncStub.subscribe(req.build(), new StreamObserver<SubscribeResponse>() {

            @Override
            public void onNext(final SubscribeResponse resp) {
                if (cancelledRef[0]) {
                    return;
                }

                try {
                    sem.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    cancelledRef[0] = true;
                    doneLatch.countDown();
                    return;
                }

                final Supplier<CompletableFuture<Void>> ackFn = () -> {
                    final CompletableFuture<Void> cf = new CompletableFuture<>();
                    final ListenableFuture<AckResponse> lf = futureStub.ack(
                            AckRequest.newBuilder()
                                    .setId(resp.getId())
                                    .setConsumerGroup(options.getConsumerGroup())
                                    .build());
                    lf.addListener(() -> {
                        try {
                            lf.get();
                            cf.complete(null);
                        } catch (ExecutionException e) {
                            cf.completeExceptionally(e.getCause());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            cf.completeExceptionally(e);
                        }
                    }, MoreExecutors.directExecutor());
                    return cf;
                };

                final Function<String, CompletableFuture<Void>> nackFn = reason -> {
                    final CompletableFuture<Void> cf = new CompletableFuture<>();
                    final ListenableFuture<NackResponse> lf = futureStub.nack(
                            NackRequest.newBuilder()
                                    .setId(resp.getId())
                                    .setError(reason)
                                    .setConsumerGroup(options.getConsumerGroup())
                                    .build());
                    lf.addListener(() -> {
                        try {
                            lf.get();
                            cf.complete(null);
                        } catch (ExecutionException e) {
                            cf.completeExceptionally(e.getCause());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            cf.completeExceptionally(e);
                        }
                    }, MoreExecutors.directExecutor());
                    return cf;
                };

                final Message msg = Message.fromSubscribeResponse(resp, ackFn, nackFn);
                executor.submit(() -> {
                    try {
                        dispatch(msg, handler, cancelledRef);
                    } finally {
                        sem.release();
                    }
                });
            }

            @Override
            public void onError(final Throwable t) {
                if (cancelledRef[0]) {
                    return;
                }
                logger.warning("queue-ti consumer: stream error on topic '" + topic
                        + "', reconnecting in " + backoff[0].toMillis() + "ms: " + t);
                scheduleConnect(backoff[0], cancelledRef, sem, executor, scheduler,
                                doneLatch, backoff, handler);
                backoff[0] = nextBackoff(backoff[0]);
            }

            @Override
            public void onCompleted() {
                if (cancelledRef[0]) {
                    return;
                }
                // Server closed cleanly — reconnect immediately and reset backoff.
                backoff[0] = BACKOFF_START;
                scheduleConnect(Duration.ZERO, cancelledRef, sem, executor, scheduler,
                                doneLatch, backoff, handler);
            }
        });
    }

    /**
     * Dispatches a single message to {@code handler}.
     *
     * <p>If the handler returns normally, the message is acked. If it throws, the message
     * is nacked with the throwable's message as the reason.
     *
     * @param msg         the message to dispatch
     * @param handler     the application-provided handler
     * @param cancelledRef single-element array holding the cancelled flag
     */
    private void dispatch(
            final Message msg,
            final MessageHandler handler,
            final boolean[] cancelledRef) {
        try {
            handler.handle(msg);
            msg.ack().whenComplete((v, err) -> {
                if (err != null && !cancelledRef[0]) {
                    logger.warning("queue-ti consumer: ack failed for "
                            + msg.id() + ": " + err);
                }
            });
        } catch (Throwable t) {
            final String reason = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
            msg.nack(reason).whenComplete((v, err) -> {
                if (err != null && !cancelledRef[0]) {
                    logger.warning("queue-ti consumer: nack failed for "
                            + msg.id() + ": " + err);
                }
            });
        }
    }

    /**
     * Computes the next exponential-backoff duration, capped at {@link #BACKOFF_MAX}.
     *
     * @param current the current backoff duration
     * @return {@code min(current * 2, BACKOFF_MAX)}
     */
    static Duration nextBackoff(final Duration current) {
        final Duration doubled = current.multipliedBy(2);
        return doubled.compareTo(BACKOFF_MAX) < 0 ? doubled : BACKOFF_MAX;
    }
}
