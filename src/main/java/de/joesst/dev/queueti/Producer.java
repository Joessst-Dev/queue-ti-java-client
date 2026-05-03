package de.joesst.dev.queueti;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import de.joesst.dev.queueti.pb.EnqueueRequest;
import de.joesst.dev.queueti.pb.EnqueueResponse;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Publishes messages to queue-ti topics.
 *
 * <p>Instances are obtained via {@link QueueTiClient#newProducer()} and share the parent
 * client's channel and credentials. All {@code publish} calls are non-blocking — they
 * return a {@link CompletableFuture} that completes with the server-assigned message ID
 * on success, or completes exceptionally on any RPC failure.
 *
 * <p>Thread-safe: {@code publish} may be called concurrently from multiple threads.
 */
public final class Producer {

    private final QueueServiceGrpc.QueueServiceFutureStub stub;

    /**
     * Package-private constructor — callers must use {@link QueueTiClient#newProducer()}.
     *
     * @param stub the future stub used for RPC calls; must not be {@code null}
     */
    Producer(final QueueServiceGrpc.QueueServiceFutureStub stub) {
        this.stub = stub;
    }

    /**
     * Publishes {@code payload} to {@code topic} with default publish options.
     *
     * <p>Equivalent to {@code publish(topic, payload, PublishOptions.builder().build())}.
     *
     * @param topic   the destination topic; must not be {@code null} or blank
     * @param payload the raw message bytes; must not be {@code null}
     * @return a {@link CompletableFuture} that completes with the server-assigned message ID
     * @throws IllegalArgumentException if {@code topic} is {@code null} or blank, or
     *                                  {@code payload} is {@code null}
     */
    public CompletableFuture<String> publish(final String topic, final byte[] payload) {
        return publish(topic, payload, PublishOptions.builder().build());
    }

    /**
     * Publishes {@code payload} to {@code topic} with the supplied options.
     *
     * <p>Validation is performed eagerly, before any RPC is initiated. On success the returned
     * future completes with the server-assigned message ID string. On RPC failure the future
     * completes exceptionally with the underlying {@link io.grpc.StatusRuntimeException}.
     *
     * @param topic   the destination topic; must not be {@code null} or blank
     * @param payload the raw message bytes; must not be {@code null}
     * @param options publish configuration (metadata, routing key); must not be {@code null}
     * @return a {@link CompletableFuture} that completes with the server-assigned message ID
     * @throws IllegalArgumentException if {@code topic} is {@code null} or blank, or
     *                                  {@code payload} is {@code null}
     */
    public CompletableFuture<String> publish(
            final String topic,
            final byte[] payload,
            final PublishOptions options) {

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }

        final EnqueueRequest.Builder req = EnqueueRequest.newBuilder()
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(payload))
                .putAllMetadata(options.getMetadata());
        if (options.getKey() != null) {
            req.setKey(options.getKey());
        }

        final ListenableFuture<EnqueueResponse> lf = stub.enqueue(req.build());
        final CompletableFuture<String> cf = new CompletableFuture<>();
        lf.addListener(() -> {
            try {
                cf.complete(lf.get().getId());
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
