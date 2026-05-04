package de.joesst.dev.queueti;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptionsTest {

    // ── ConnectOptions ────────────────────────────────────────────────────────

    @Test
    @DisplayName("ConnectOptions.defaults() has insecure=false and null token")
    void connectOptions_defaults_are_insecure_false_and_null_token() {
        // Given / When
        var opts = ConnectOptions.defaults();

        // Then
        assertThat(opts.isInsecure()).isFalse();
        assertThat(opts.getToken()).isNull();
        assertThat(opts.getTokenRefresher()).isNull();
    }

    @Test
    @DisplayName("ConnectOptions.builder() sets all fields correctly")
    void connectOptions_builder_sets_all_fields() {
        // Given
        TokenRefresher refresher = () -> java.util.concurrent.CompletableFuture.completedFuture("t");

        // When
        var opts = ConnectOptions.builder()
                .insecure(true)
                .token("my-jwt")
                .tokenRefresher(refresher)
                .build();

        // Then
        assertThat(opts.isInsecure()).isTrue();
        assertThat(opts.getToken()).isEqualTo("my-jwt");
        assertThat(opts.getTokenRefresher()).isSameAs(refresher);
    }

    // ── PublishOptions ────────────────────────────────────────────────────────

    @Test
    @DisplayName("PublishOptions default metadata is an empty map")
    void publishOptions_defaults_metadata_is_empty_map() {
        // Given / When
        var opts = PublishOptions.builder().build();

        // Then
        assertThat(opts.getMetadata()).isEmpty();
        assertThat(opts.getKey()).isNull();
    }

    @Test
    @DisplayName("PublishOptions coerces null metadata to an empty map")
    void publishOptions_null_metadata_coerced_to_empty() {
        // Given
        var builder = PublishOptions.builder().metadata(null);

        // When
        var opts = builder.build();

        // Then
        assertThat(opts.getMetadata())
                .isNotNull()
                .isEmpty();
    }

    // ── ConsumerOptions ───────────────────────────────────────────────────────

    @Test
    @DisplayName("ConsumerOptions default concurrency is 1")
    void consumerOptions_default_concurrency_is_one() {
        // Given / When
        var opts = ConsumerOptions.builder().build();

        // Then
        assertThat(opts.getConcurrency()).isEqualTo(1);
    }

    @Test
    @DisplayName("ConsumerOptions.builder().concurrency(0).build() throws IllegalArgumentException")
    void consumerOptions_throws_for_zero_concurrency() {
        // Given
        var builder = ConsumerOptions.builder().concurrency(0);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrency must be >= 1");
    }

    @Test
    @DisplayName("ConsumerOptions.builder().concurrency(-1).build() throws IllegalArgumentException")
    void consumerOptions_throws_for_negative_concurrency() {
        // Given
        var builder = ConsumerOptions.builder().concurrency(-5);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrency must be >= 1");
    }

    // ── BatchOptions ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("BatchOptions default consumerGroup is an empty string")
    void batchOptions_default_consumer_group_is_empty_string() {
        // Given / When
        var opts = BatchOptions.builder().build();

        // Then
        assertThat(opts.getConsumerGroup()).isEqualTo("");
    }

    // ── ConsumerOptions (nullable timeout) ────────────────────────────────────

    @Test
    @DisplayName("ConsumerOptions visibilityTimeoutSeconds is null by default")
    void consumerOptions_visibility_timeout_is_null_by_default() {
        // Given / When
        var opts = ConsumerOptions.builder().build();

        // Then
        assertThat(opts.getVisibilityTimeoutSeconds()).isNull();
    }

    @Test
    @DisplayName("PublishOptions metadata is defensively copied from the builder map")
    void publishOptions_metadata_is_defensively_copied() {
        // Given
        final var original = new HashMap<String, String>();
        original.put("k", "v");
        final var opts = PublishOptions.builder().metadata(original).build();

        // When — mutate the original map after build
        original.put("extra", "should-not-appear");

        // Then — getMetadata() must not reflect the post-build mutation
        assertThat(opts.getMetadata())
                .containsOnlyKeys("k")
                .doesNotContainKey("extra");
    }
}
