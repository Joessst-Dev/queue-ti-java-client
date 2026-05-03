package de.joesst.dev.queueti;

import java.util.concurrent.CompletableFuture;

/**
 * Strategy for obtaining a fresh JWT access token asynchronously.
 *
 * <p>Implementations are responsible for all authentication details (e.g. OAuth2 client
 * credentials, a static token rotation, or any other scheme). The returned future must
 * complete with a non-null, valid JWT string.
 *
 * <p>Typical usage:
 * <pre>{@code
 * TokenRefresher refresher = () -> myAuthClient.fetchToken();
 * ConnectOptions options = ConnectOptions.builder()
 *         .tokenRefresher(refresher)
 *         .build();
 * }</pre>
 */
@FunctionalInterface
public interface TokenRefresher {

    /**
     * Requests a fresh token.
     *
     * @return a future that completes with the new JWT string; must not complete with {@code null}
     */
    CompletableFuture<String> refresh();
}
