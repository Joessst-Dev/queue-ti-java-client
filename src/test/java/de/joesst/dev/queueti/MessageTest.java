package de.joesst.dev.queueti;

import com.google.protobuf.Timestamp;
import de.joesst.dev.queueti.pb.SubscribeResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageTest {

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Returns a minimal {@link Message} using the package-private constructor.
     * Ack and nack functions complete immediately with a void result.
     */
    private static Message minimalMessage(
            final byte[] payload,
            final Map<String, String> metadata) {
        return new Message(
                "id-1",
                "test-topic",
                payload,
                metadata,
                Instant.EPOCH,
                0,
                () -> CompletableFuture.completedFuture(null),
                reason -> CompletableFuture.completedFuture(null));
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("payload accessor returns a defensive copy")
    void payload_accessor_returns_defensive_copy() {
        // Given
        final byte[] originalBytes = {1, 2, 3};
        final var msg = minimalMessage(originalBytes, Map.of());

        // When — mutate the array returned by the first call
        final byte[] firstResult = msg.payload();
        firstResult[0] = 99;

        // Then — a subsequent call still returns the original values
        assertThat(msg.payload()).containsExactly(1, 2, 3);
    }

    @Test
    @DisplayName("metadata accessor returns an unmodifiable view")
    void metadata_accessor_returns_unmodifiable_view() {
        // Given
        final var msg = minimalMessage(new byte[0], Map.of("key", "value"));

        // When
        final var metadata = msg.metadata();

        // Then
        assertThatThrownBy(() -> metadata.put("extra", "entry"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("createdAt is Instant.EPOCH when SubscribeResponse has no created_at field")
    void createdAt_is_epoch_when_proto_timestamp_is_absent() {
        // Given — a SubscribeResponse with no created_at set
        final var resp = SubscribeResponse.newBuilder()
                .setId("abc")
                .setTopic("t")
                .build();

        // When
        final var msg = Message.fromSubscribeResponse(
                resp,
                () -> CompletableFuture.completedFuture(null),
                reason -> CompletableFuture.completedFuture(null));

        // Then
        assertThat(msg.createdAt()).isEqualTo(Instant.EPOCH);
    }

    @Test
    @DisplayName("createdAt maps correctly from a proto Timestamp in SubscribeResponse")
    void createdAt_maps_correctly_from_proto_timestamp() {
        // Given
        final long seconds = 1_700_000_000L;
        final var ts = Timestamp.newBuilder().setSeconds(seconds).setNanos(0).build();
        final var resp = SubscribeResponse.newBuilder()
                .setId("abc")
                .setTopic("t")
                .setCreatedAt(ts)
                .build();

        // When
        final var msg = Message.fromSubscribeResponse(
                resp,
                () -> CompletableFuture.completedFuture(null),
                reason -> CompletableFuture.completedFuture(null));

        // Then
        assertThat(msg.createdAt()).isEqualTo(Instant.ofEpochSecond(seconds));
    }

    @Test
    @DisplayName("ack() delegates to the ackFn closure")
    void ack_delegates_to_ack_fn() {
        // Given
        final var ackInvoked = new CompletableFuture<Void>();
        final var msg = new Message(
                "id-2",
                "t",
                new byte[0],
                null,
                Instant.EPOCH,
                0,
                () -> {
                    ackInvoked.complete(null);
                    return ackInvoked;
                },
                reason -> CompletableFuture.completedFuture(null));

        // When
        msg.ack();

        // Then
        assertThat(ackInvoked).isDone();
    }

    @Test
    @DisplayName("nack(reason) delegates to the nackFn closure with the supplied reason")
    void nack_delegates_to_nack_fn_with_reason() {
        // Given
        final var capturedReason = new AtomicReference<String>();
        final var msg = new Message(
                "id-3",
                "t",
                new byte[0],
                null,
                Instant.EPOCH,
                0,
                () -> CompletableFuture.completedFuture(null),
                reason -> {
                    capturedReason.set(reason);
                    return CompletableFuture.completedFuture(null);
                });

        // When
        msg.nack("err");

        // Then
        assertThat(capturedReason.get()).isEqualTo("err");
    }

    @Test
    @DisplayName("null metadata passed to the constructor becomes an empty, non-null map")
    void metadata_null_from_caller_becomes_empty_map() {
        // Given / When
        final var msg = minimalMessage(new byte[0], null);

        // Then
        assertThat(msg.metadata())
                .isNotNull()
                .isEmpty();
    }
}
