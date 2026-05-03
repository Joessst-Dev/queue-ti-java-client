package de.joesst.dev.queueti;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.SecurityLevel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;

class BearerTokenCredentialsTest {

    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /** Synchronous executor — runs tasks inline so tests need no async coordination. */
    private static final Executor DIRECT_EXECUTOR = Runnable::run;

    /**
     * Captures the single {@link Metadata} argument passed to {@link #apply(Metadata)}.
     * Stays {@code null} if {@code apply} was never invoked.
     */
    private static class CapturingApplier extends CallCredentials.MetadataApplier {
        Metadata captured = null;

        @Override
        public void apply(final Metadata headers) {
            this.captured = headers;
        }

        @Override
        public void fail(final io.grpc.Status status) {
            throw new AssertionError("fail() was called unexpectedly: " + status);
        }
    }

    /** Minimal {@link CallCredentials.RequestInfo} that satisfies the interface contract. */
    private static CallCredentials.RequestInfo fakeRequestInfo() {
        return new CallCredentials.RequestInfo() {
            @Override
            public MethodDescriptor<?, ?> getMethodDescriptor() {
                return null;
            }

            @Override
            public SecurityLevel getSecurityLevel() {
                return SecurityLevel.NONE;
            }

            @Override
            public String getAuthority() {
                return "localhost";
            }

            @Override
            public io.grpc.Attributes getTransportAttrs() {
                return io.grpc.Attributes.EMPTY;
            }
        };
    }

    @Test
    @DisplayName("apply adds Authorization header with the current token when a token is present")
    void apply_adds_authorization_header_when_token_present() {
        // Given
        var store = new TokenStore("my-token");
        var credentials = new BearerTokenCredentials(store);
        var applier = new CapturingApplier();

        // When
        credentials.applyRequestMetadata(fakeRequestInfo(), DIRECT_EXECUTOR, applier);

        // Then
        assertThat(applier.captured).isNotNull();
        assertThat(applier.captured.get(AUTHORIZATION_KEY)).isEqualTo("Bearer my-token");
    }

    @Test
    @DisplayName("apply sends empty metadata and no Authorization header when the token is null")
    void apply_skips_header_when_token_is_null() {
        // Given
        var store = new TokenStore(null);
        var credentials = new BearerTokenCredentials(store);
        var applier = new CapturingApplier();

        // When
        credentials.applyRequestMetadata(fakeRequestInfo(), DIRECT_EXECUTOR, applier);

        // Then
        assertThat(applier.captured).isNotNull();
        assertThat(applier.captured.get(AUTHORIZATION_KEY)).isNull();
    }

    @Test
    @DisplayName("apply reflects a token that was updated after construction")
    void apply_reads_token_from_store_at_call_time() {
        // Given — store starts with one token
        var store = new TokenStore("original-token");
        var credentials = new BearerTokenCredentials(store);

        // When — token is replaced after credentials are constructed
        store.set("updated-token");
        var applier = new CapturingApplier();
        credentials.applyRequestMetadata(fakeRequestInfo(), DIRECT_EXECUTOR, applier);

        // Then — header reflects the updated value, not the original
        assertThat(applier.captured.get(AUTHORIZATION_KEY)).isEqualTo("Bearer updated-token");
    }

    @Test
    @DisplayName("apply prefixes the token value with 'Bearer '")
    void apply_uses_bearer_prefix() {
        // Given
        var store = new TokenStore("some.jwt.token");
        var credentials = new BearerTokenCredentials(store);
        var applier = new CapturingApplier();

        // When
        credentials.applyRequestMetadata(fakeRequestInfo(), DIRECT_EXECUTOR, applier);

        // Then
        assertThat(applier.captured.get(AUTHORIZATION_KEY)).startsWith("Bearer ");
    }
}
