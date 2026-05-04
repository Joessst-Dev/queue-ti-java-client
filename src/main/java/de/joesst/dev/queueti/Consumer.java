package de.joesst.dev.queueti;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import de.joesst.dev.queueti.pb.AckRequest;
import de.joesst.dev.queueti.pb.BatchDequeueRequest;
import de.joesst.dev.queueti.pb.DequeueResponse;
import de.joesst.dev.queueti.pb.NackRequest;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import de.joesst.dev.queueti.pb.SubscribeRequest;
import de.joesst.dev.queueti.pb.SubscribeResponse;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    // ── StreamSession: live state for one streaming session ───────────────────

    /** Bundles all mutable state for a streaming lifecycle; passed as one argument. */
    private record StreamSession(
            AtomicBoolean cancelled,
            Semaphore sem,
            ExecutorService executor,
            ScheduledExecutorService scheduler,
            CountDownLatch doneLatch,
            AtomicReference<Duration> backoff,
            MessageHandler handler) {}

    // ── Public API ────────────────────────────────────────────────────────────

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
        final var session = new StreamSession(
                new AtomicBoolean(false),
                new Semaphore(options.getConcurrency()),
                Executors.newVirtualThreadPerTaskExecutor(),
                Executors.newSingleThreadScheduledExecutor(
                        r -> Thread.ofVirtual().name("queue-ti-reconnect-scheduler").unstarted(r)),
                new CountDownLatch(1),
                new AtomicReference<>(BACKOFF_START),
                handler);

        scheduleConnect(Duration.ZERO, session);

        try {
            session.doneLatch().await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            session.cancelled().set(true);
        }

        session.cancelled().set(true);
        session.scheduler().shutdownNow();
        session.executor().shutdown();
        try {
            session.executor().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the batch-polling consumer loop, delivering each batch of messages to
     * {@code handler}.
     *
     * <p>This method blocks until the calling thread is interrupted. On each poll it
     * requests up to {@code batchSize} messages from the server. When the server returns
     * an empty batch, the consumer backs off using exponential backoff starting at
     * {@value #BACKOFF_START_MS} ms and capped at {@value #BACKOFF_MAX_SECS} s.
     * Backoff resets to the starting value immediately after a non-empty batch is received.
     *
     * <p>If {@code handler} throws any {@link Throwable}, every message in the batch that
     * has not already been nacked is nacked with the throwable's message as the reason.
     *
     * <p>Consumer group and visibility timeout are taken from the {@link ConsumerOptions}
     * supplied at construction time.
     *
     * @param batchSize maximum number of messages to request per poll; must be {@code >= 1}
     * @param handler   handler invoked with each non-empty batch; must not be {@code null}
     * @throws IllegalArgumentException if {@code batchSize < 1}
     */
    public void consumeBatch(final int batchSize, final BatchMessageHandler handler) {
        final BatchOptions bo = BatchOptions.builder()
                .consumerGroup(options.getConsumerGroup())
                .visibilityTimeoutSeconds(options.getVisibilityTimeoutSeconds())
                .build();
        consumeBatch(batchSize, handler, bo);
    }

    /**
     * Starts the batch-polling consumer loop, delivering each batch of messages to
     * {@code handler}, using the supplied {@link BatchOptions} to override the consumer
     * group and visibility timeout configured at construction time.
     *
     * <p>This method blocks until the calling thread is interrupted.
     *
     * @param batchSize    maximum number of messages to request per poll; must be {@code >= 1}
     * @param handler      handler invoked with each non-empty batch; must not be {@code null}
     * @param batchOptions per-call overrides for consumer group and visibility timeout;
     *                     must not be {@code null}
     * @throws IllegalArgumentException if {@code batchSize < 1}
     */
    public void consumeBatch(final int batchSize, final BatchMessageHandler handler,
                             final BatchOptions batchOptions) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        Duration backoff = BACKOFF_START;
        while (!Thread.currentThread().isInterrupted()) {
            final BatchDequeueRequest.Builder req = BatchDequeueRequest.newBuilder()
                    .setTopic(topic)
                    .setCount(batchSize)
                    .setConsumerGroup(batchOptions.getConsumerGroup());
            if (batchOptions.getVisibilityTimeoutSeconds() != null) {
                req.setVisibilityTimeoutSeconds(batchOptions.getVisibilityTimeoutSeconds());
            }

            final List<DequeueResponse> rawMessages;
            try {
                rawMessages = futureStub.batchDequeue(req.build()).get().getMessagesList();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                logger.warning("queue-ti consumer: batchDequeue error (retrying in "
                        + backoff + "): " + e.getCause());
                sleep(backoff);
                backoff = nextBackoff(backoff);
                continue;
            }

            if (rawMessages.isEmpty()) {
                sleep(backoff);
                backoff = nextBackoff(backoff);
                continue;
            }

            backoff = BACKOFF_START;

            final Set<String> ackedIds  = Collections.synchronizedSet(new HashSet<>());
            final Set<String> nackedIds = Collections.synchronizedSet(new HashSet<>());
            final List<Message> messages = new ArrayList<>(rawMessages.size());
            for (final DequeueResponse raw : rawMessages) {
                messages.add(buildBatchMessage(raw, batchOptions, ackedIds, nackedIds));
            }

            try {
                handler.handle(messages);
                for (final Message msg : messages) {
                    if (!ackedIds.contains(msg.id()) && !nackedIds.contains(msg.id())) {
                        msg.ack().whenComplete((v, err) -> {
                            if (err != null) {
                                logger.warning("queue-ti consumer: batch ack failed: " + err);
                            }
                        });
                    }
                }
            } catch (Throwable t) {
                final String reason = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
                for (final Message msg : messages) {
                    if (!nackedIds.contains(msg.id())) {
                        msg.nack(reason).whenComplete((v, err) -> {
                            if (err != null) {
                                logger.warning("queue-ti consumer: batch nack failed: " + err);
                            }
                        });
                    }
                }
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Schedules a call to {@link #openStream} after {@code delay}.
     */
    private void scheduleConnect(final Duration delay, final StreamSession session) {
        session.scheduler().schedule(
                () -> openStream(session),
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Opens one server-streaming Subscribe call and wires a {@link StreamObserver} that
     * handles reconnection on error or clean close.
     */
    private void openStream(final StreamSession session) {
        if (session.cancelled().get()) {
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
                if (session.cancelled().get()) {
                    return;
                }
                try {
                    session.sem().acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    session.cancelled().set(true);
                    session.doneLatch().countDown();
                    return;
                }

                final Supplier<CompletableFuture<Void>> ackFn = () ->
                        toVoidFuture(futureStub.ack(AckRequest.newBuilder()
                                .setId(resp.getId())
                                .setConsumerGroup(options.getConsumerGroup())
                                .build()));

                final Function<String, CompletableFuture<Void>> nackFn = reason ->
                        toVoidFuture(futureStub.nack(NackRequest.newBuilder()
                                .setId(resp.getId())
                                .setError(reason)
                                .setConsumerGroup(options.getConsumerGroup())
                                .build()));

                final Message msg = Message.fromSubscribeResponse(resp, ackFn, nackFn);
                session.executor().submit(() -> {
                    try {
                        dispatch(msg, session.handler(), session.cancelled());
                    } finally {
                        session.sem().release();
                    }
                });
            }

            @Override
            public void onError(final Throwable t) {
                if (session.cancelled().get()) {
                    return;
                }
                final Duration current = session.backoff().get();
                logger.warning("queue-ti consumer: stream error on topic '" + topic
                        + "', reconnecting in " + current.toMillis() + "ms: " + t);
                scheduleConnect(current, session);
                session.backoff().set(nextBackoff(current));
            }

            @Override
            public void onCompleted() {
                if (session.cancelled().get()) {
                    return;
                }
                session.backoff().set(BACKOFF_START);
                scheduleConnect(Duration.ZERO, session);
            }
        });
    }

    /** Builds a {@link Message} from a batch dequeue response, wiring ack/nack to track settlement. */
    private Message buildBatchMessage(
            final DequeueResponse raw,
            final BatchOptions batchOptions,
            final Set<String> ackedIds,
            final Set<String> nackedIds) {
        final String msgId = raw.getId();
        final Supplier<CompletableFuture<Void>> ackFn = () -> {
            ackedIds.add(msgId);
            return toVoidFuture(futureStub.ack(AckRequest.newBuilder()
                    .setId(msgId)
                    .setConsumerGroup(batchOptions.getConsumerGroup())
                    .build()));
        };
        final Function<String, CompletableFuture<Void>> nackFn = reason -> {
            nackedIds.add(msgId);
            return toVoidFuture(futureStub.nack(NackRequest.newBuilder()
                    .setId(msgId)
                    .setError(reason)
                    .setConsumerGroup(batchOptions.getConsumerGroup())
                    .build()));
        };
        return Message.fromDequeueResponse(raw, ackFn, nackFn);
    }

    /**
     * Dispatches a single message to {@code handler}.
     *
     * <p>If the handler returns normally, the message is acked. If it throws, the message
     * is nacked with the throwable's message as the reason.
     *
     * @param msg          the message to dispatch
     * @param handler      the application-provided handler
     * @param cancelledRef atomic flag indicating whether the consumer has been cancelled
     */
    private void dispatch(
            final Message msg,
            final MessageHandler handler,
            final AtomicBoolean cancelledRef) {
        try {
            handler.handle(msg);
            msg.ack().whenComplete((v, err) -> {
                if (err != null && !cancelledRef.get()) {
                    logger.warning("queue-ti consumer: ack failed for "
                            + msg.id() + ": " + err);
                }
            });
        } catch (Throwable t) {
            final String reason = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
            msg.nack(reason).whenComplete((v, err) -> {
                if (err != null && !cancelledRef.get()) {
                    logger.warning("queue-ti consumer: nack failed for "
                            + msg.id() + ": " + err);
                }
            });
        }
    }

    /**
     * Sleeps for the specified duration, re-interrupting the thread if interrupted.
     *
     * @param d the duration to sleep; negative or zero values are silently ignored
     */
    private void sleep(final Duration d) {
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    /** Bridges a {@link ListenableFuture} to a {@link CompletableFuture}{@code <Void>}. */
    private static <T> CompletableFuture<Void> toVoidFuture(final ListenableFuture<T> lf) {
        final CompletableFuture<Void> cf = new CompletableFuture<>();
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
    }
}
