package de.joesst.dev.queueti;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

import java.util.concurrent.Executor;

/**
 * gRPC {@link CallCredentials} that injects a Bearer token from a {@link TokenStore} into every
 * outbound call's metadata.
 *
 * <p>The token is read from the store at call time, so a token rotation applied via
 * {@link TokenStore#set(String)} is immediately reflected in subsequent RPCs without rebuilding
 * the channel or stubs.
 *
 * <p>When the store holds a {@code null} or blank token no {@code Authorization} header is added,
 * allowing unauthenticated calls to pass through (e.g. during startup before the first token is
 * available).
 */
final class BearerTokenCredentials extends CallCredentials {

    private static final Metadata.Key<String> AUTHORIZATION_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final TokenStore store;

    /**
     * Constructs credentials backed by the supplied token store.
     *
     * @param store the store from which the current token is read on each RPC; must not be
     *              {@code null}
     */
    BearerTokenCredentials(final TokenStore store) {
        this.store = store;
    }

    /**
     * Called by the gRPC framework before each RPC to apply per-call metadata.
     *
     * <p>The token is fetched from the store and — when non-null and non-blank — written into a
     * fresh {@link Metadata} as {@code Authorization: Bearer <token>}. When there is no valid
     * token an empty {@code Metadata} is applied so the call still proceeds without authentication.
     *
     * @param requestInfo metadata about the outgoing RPC (channel, method, security level)
     * @param appExecutor the executor provided by the channel for off-thread work
     * @param applier     the callback that must be invoked exactly once with the prepared metadata
     */
    @Override
    public void applyRequestMetadata(
            final RequestInfo requestInfo,
            final Executor appExecutor,
            final MetadataApplier applier) {

        appExecutor.execute(() -> {
            final String token = store.get();
            if (token == null || token.isBlank()) {
                applier.apply(new Metadata());
                return;
            }
            final var headers = new Metadata();
            headers.put(AUTHORIZATION_KEY, "Bearer " + token);
            applier.apply(headers);
        });
    }
}
