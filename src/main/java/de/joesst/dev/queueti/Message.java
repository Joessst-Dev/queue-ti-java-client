package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.DequeueResponse;
import de.joesst.dev.queueti.pb.SubscribeResponse;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An immutable message received from a queue-ti topic.
 *
 * <p>Payload and metadata are defensively copied on construction so that
 * mutations to the originating proto bytes or map cannot corrupt internal state.
 *
 * <p>Instances are created by the library via the package-private constructor or
 * the static factory methods {@link #fromSubscribeResponse} and
 * {@link #fromDequeueResponse}. Consumers of this class should never need to
 * construct one directly.
 *
 * <p>This class is effectively immutable and therefore thread-safe, provided
 * that the {@code ackFn} and {@code nackFn} closures are themselves thread-safe.
 */
public final class Message {

    private final String id;
    private final String topic;
    private final byte[] payload;
    private final Map<String, String> metadata;
    private final Instant createdAt;
    private final int retryCount;
    private final Supplier<CompletableFuture<Void>> ackFn;
    private final Function<String, CompletableFuture<Void>> nackFn;

    /**
     * Package-private constructor — called by the library and factory methods.
     *
     * @param id         unique message identifier
     * @param topic      topic the message was published to
     * @param payload    raw message bytes; defensive copy is made; {@code null} is treated as empty
     * @param metadata   string key/value pairs; defensive copy is made; {@code null} becomes an empty map
     * @param createdAt  wall-clock time the message was enqueued
     * @param retryCount number of previous delivery attempts
     * @param ackFn      closure to acknowledge successful processing
     * @param nackFn     closure to negative-acknowledge with an error reason
     */
    Message(
            final String id,
            final String topic,
            final byte[] payload,
            final Map<String, String> metadata,
            final Instant createdAt,
            final int retryCount,
            final Supplier<CompletableFuture<Void>> ackFn,
            final Function<String, CompletableFuture<Void>> nackFn) {
        this.id = id;
        this.topic = topic;
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
        this.createdAt = createdAt;
        this.retryCount = retryCount;
        this.ackFn = ackFn;
        this.nackFn = nackFn;
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /**
     * Returns the unique message identifier assigned by the server.
     *
     * @return message id; never {@code null}
     */
    public String id() {
        return id;
    }

    /**
     * Returns the topic this message was published to.
     *
     * @return topic name; never {@code null}
     */
    public String topic() {
        return topic;
    }

    /**
     * Returns a defensive copy of the raw payload bytes.
     *
     * <p>A new array is returned on every call; mutating the result has no effect
     * on the {@code Message} instance.
     *
     * @return copy of the payload; never {@code null}, may be empty
     */
    public byte[] payload() {
        return payload.clone();
    }

    /**
     * Returns an unmodifiable view of the message metadata.
     *
     * @return metadata map; never {@code null}, may be empty
     */
    public Map<String, String> metadata() {
        return metadata;
    }

    /**
     * Returns the wall-clock time at which the message was enqueued.
     *
     * @return creation timestamp; {@link Instant#EPOCH} when the server did not supply one
     */
    public Instant createdAt() {
        return createdAt;
    }

    /**
     * Returns the number of previous delivery attempts for this message.
     *
     * @return retry count; {@code 0} on first delivery
     */
    public int retryCount() {
        return retryCount;
    }

    // ── Ack / Nack ────────────────────────────────────────────────────────────

    /**
     * Acknowledges that the message was processed successfully.
     *
     * @return a future that completes when the server confirms the ack
     */
    public CompletableFuture<Void> ack() {
        return ackFn.get();
    }

    /**
     * Negative-acknowledges the message, signalling a processing failure.
     *
     * @param reason human-readable description of why processing failed
     * @return a future that completes when the server confirms the nack
     */
    public CompletableFuture<Void> nack(final String reason) {
        return nackFn.apply(reason);
    }

    // ── Static factories ──────────────────────────────────────────────────────

    /**
     * Constructs a {@code Message} from a streaming {@link SubscribeResponse} proto.
     *
     * @param resp  proto message received from the server
     * @param ackFn closure to invoke when the caller calls {@link #ack()}
     * @param nackFn closure to invoke when the caller calls {@link #nack(String)}
     * @return a fully populated {@code Message}
     */
    static Message fromSubscribeResponse(
            final SubscribeResponse resp,
            final Supplier<CompletableFuture<Void>> ackFn,
            final Function<String, CompletableFuture<Void>> nackFn) {
        final Instant createdAt = resp.hasCreatedAt()
                ? Instant.ofEpochSecond(resp.getCreatedAt().getSeconds(), resp.getCreatedAt().getNanos())
                : Instant.EPOCH;
        return new Message(
                resp.getId(),
                resp.getTopic(),
                resp.getPayload().toByteArray(),
                resp.getMetadataMap(),
                createdAt,
                resp.getRetryCount(),
                ackFn,
                nackFn);
    }

    /**
     * Constructs a {@code Message} from a polling {@link DequeueResponse} proto.
     *
     * @param resp   proto message received from the server
     * @param ackFn  closure to invoke when the caller calls {@link #ack()}
     * @param nackFn closure to invoke when the caller calls {@link #nack(String)}
     * @return a fully populated {@code Message}
     */
    static Message fromDequeueResponse(
            final DequeueResponse resp,
            final Supplier<CompletableFuture<Void>> ackFn,
            final Function<String, CompletableFuture<Void>> nackFn) {
        final Instant createdAt = resp.hasCreatedAt()
                ? Instant.ofEpochSecond(resp.getCreatedAt().getSeconds(), resp.getCreatedAt().getNanos())
                : Instant.EPOCH;
        return new Message(
                resp.getId(),
                resp.getTopic(),
                resp.getPayload().toByteArray(),
                resp.getMetadataMap(),
                createdAt,
                resp.getRetryCount(),
                ackFn,
                nackFn);
    }
}
