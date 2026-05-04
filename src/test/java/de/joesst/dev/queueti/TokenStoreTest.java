package de.joesst.dev.queueti;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenStoreTest {

    // A minimal JWT: header.payload.signature — only the payload segment matters for parseExpiry.
    // Payload {"exp":1700000000} compact → Base64URL (no padding, len=24, rem=0): eyJleHAiOjE3MDAwMDAwMDB9
    private static final String VALID_JWT =
            "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3MDAwMDAwMDB9.signature";

    // Payload {"iss":"x","exp":1700000000} → Base64URL (no padding, len=38, rem=2): needs == padding
    private static final String PADDING_JWT =
            "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJ4IiwiZXhwIjoxNzAwMDAwMDAwfQ.signature";

    // Payload {"sub":"user"} — no exp field
    private static final String NO_EXP_JWT =
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature";

    @Test
    @DisplayName("get returns the initial token supplied at construction time")
    void get_returns_initial_token() {
        // Given
        var store = new TokenStore("initial-token");

        // When
        var result = store.get();

        // Then
        assertThat(result).isEqualTo("initial-token");
    }

    @Test
    @DisplayName("set replaces the stored token and the new value is returned by get")
    void set_replaces_token() {
        // Given
        var store = new TokenStore("old-token");

        // When
        store.set("new-token");

        // Then
        assertThat(store.get()).isEqualTo("new-token");
    }

    @Test
    @DisplayName("get returns null when the store is constructed with null")
    void get_returns_null_when_constructed_with_null() {
        // Given
        var store = new TokenStore(null);

        // When
        var result = store.get();

        // Then
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("parseExpiry returns the correct Instant for a well-formed JWT with an exp claim")
    void parseExpiry_returns_correct_instant_for_valid_jwt() {
        // Given — VALID_JWT payload decodes to {"exp":1700000000}

        // When
        var expiry = TokenStore.parseExpiry(VALID_JWT);

        // Then
        assertThat(expiry).isEqualTo(Instant.ofEpochSecond(1_700_000_000L));
    }

    @Test
    @DisplayName("parseExpiry throws IllegalArgumentException for a JWT missing dot separators")
    void parseExpiry_throws_for_malformed_jwt_missing_dots() {
        // Given
        var malformed = "notavalidjwtatall";

        // When / Then
        assertThatThrownBy(() -> TokenStore.parseExpiry(malformed))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("parseExpiry throws IllegalArgumentException when the payload has no exp claim")
    void parseExpiry_throws_for_jwt_without_exp_claim() {
        // Given — NO_EXP_JWT payload decodes to {"sub":"user"}

        // When / Then
        assertThatThrownBy(() -> TokenStore.parseExpiry(NO_EXP_JWT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exp");
    }

    @Test
    @DisplayName("parseExpiry correctly pads a Base64URL payload whose unpadded length is not a multiple of 4")
    void parseExpiry_handles_base64_padding() {
        // Given — PADDING_JWT payload is 38 chars (rem=2), requires == padding to decode
        // It decodes to {"iss":"x","exp":1700000000}

        // When
        var expiry = TokenStore.parseExpiry(PADDING_JWT);

        // Then
        assertThat(expiry).isEqualTo(Instant.ofEpochSecond(1_700_000_000L));
    }

    @Test
    @DisplayName("a value written by set on one virtual thread is visible to get on another")
    void set_is_visible_to_concurrent_get() throws InterruptedException {
        // Given
        var store = new TokenStore(null);
        final int threadCount = 10;
        final var errors = new AtomicReference<Throwable>(null);
        final var latch = new CountDownLatch(threadCount * 2);

        // When — 10 virtual threads set, 10 virtual threads get, all running concurrently for 200ms
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final long deadline = System.currentTimeMillis() + 200;

            for (int i = 0; i < threadCount; i++) {
                final var value = "token-" + i;
                executor.submit(() -> {
                    try {
                        while (System.currentTimeMillis() < deadline) {
                            store.set(value);
                        }
                    } catch (Exception e) {
                        errors.compareAndSet(null, e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            for (int i = 0; i < threadCount; i++) {
                executor.submit(() -> {
                    try {
                        while (System.currentTimeMillis() < deadline) {
                            store.get(); // must not throw
                        }
                    } catch (Exception e) {
                        errors.compareAndSet(null, e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
        }

        latch.await(2, TimeUnit.SECONDS);

        // Then
        assertThat(errors.get())
                .as("no exception should be thrown during concurrent access")
                .isNull();
    }
}
