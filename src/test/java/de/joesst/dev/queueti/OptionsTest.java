package de.joesst.dev.queueti;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

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
        assertThat(opts.getTlsOptions()).isNull();
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
    @DisplayName("ConnectOptions.builder() with tls sets tlsOptions and leaves insecure=false")
    void connectOptions_tls_sets_tls_options() {
        // Given
        final var tls = TlsOptions.builder().serverNameOverride("my.server").build();

        // When
        final var opts = ConnectOptions.builder().tls(tls).build();

        // Then
        assertThat(opts.getTlsOptions()).isNotNull();
        assertThat(opts.getTlsOptions().getServerNameOverride()).isEqualTo("my.server");
        assertThat(opts.isInsecure()).isFalse();
    }

    @Test
    @DisplayName("ConnectOptions.builder() with insecure=true and tls set throws")
    void connectOptions_insecure_and_tls_throws() {
        // Given
        final var tls = TlsOptions.builder().build();
        final var builder = ConnectOptions.builder().insecure(true).tls(tls);

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    // ── TlsOptions ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("TlsOptions.builder() defaults: all fields null")
    void tlsOptions_defaults_all_null() {
        // Given / When
        final var opts = TlsOptions.builder().build();

        // Then
        assertThat(opts.getRootCertificates()).isNull();
        assertThat(opts.getPrivateKey()).isNull();
        assertThat(opts.getCertificateChain()).isNull();
        assertThat(opts.getServerNameOverride()).isNull();
    }

    @Test
    @DisplayName("TlsOptions.builder() sets all fields")
    void tlsOptions_builder_sets_all_fields() {
        // Given
        final byte[] ca   = {1, 2, 3};
        final byte[] key  = {4, 5, 6};
        final byte[] cert = {7, 8, 9};

        // When
        final var opts = TlsOptions.builder()
                .rootCertificates(ca)
                .privateKey(key)
                .certificateChain(cert)
                .serverNameOverride("override.example")
                .build();

        // Then
        assertThat(opts.getRootCertificates()).containsExactly(1, 2, 3);
        assertThat(opts.getPrivateKey()).containsExactly(4, 5, 6);
        assertThat(opts.getCertificateChain()).containsExactly(7, 8, 9);
        assertThat(opts.getServerNameOverride()).isEqualTo("override.example");
    }

    @Test
    @DisplayName("TlsOptions byte arrays are defensively copied on build and on get")
    void tlsOptions_defensive_copy() {
        // Given
        final byte[] ca = {1, 2, 3};
        final var opts = TlsOptions.builder().rootCertificates(ca).build();

        // Mutate original after build
        ca[0] = 99;

        // Then — stored value must not change
        assertThat(opts.getRootCertificates()).containsExactly(1, 2, 3);

        // And getter must return an independent copy each time
        final byte[] retrieved = opts.getRootCertificates();
        retrieved[0] = 42;
        assertThat(opts.getRootCertificates()).containsExactly(1, 2, 3);
    }

    @Test
    @DisplayName("TlsOptions.builder() throws when only privateKey is set (mTLS requires both)")
    void tlsOptions_only_private_key_throws() {
        // Given
        final var builder = TlsOptions.builder().privateKey(new byte[]{1});

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both be set for mTLS");
    }

    @Test
    @DisplayName("TlsOptions.builder() throws when only certificateChain is set")
    void tlsOptions_only_certificate_chain_throws() {
        // Given
        final var builder = TlsOptions.builder().certificateChain(new byte[]{1});

        // When / Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both be set for mTLS");
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
