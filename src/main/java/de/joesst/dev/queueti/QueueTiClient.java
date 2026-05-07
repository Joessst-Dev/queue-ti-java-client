package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;
import java.io.ByteArrayInputStream;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Entry point for interacting with a queue-ti server.
 *
 * <p>Obtain an instance via {@link #connect(String, ConnectOptions)}. The client owns the
 * underlying gRPC channel and must be closed when no longer needed.
 *
 * <p>Thread-safe: all public methods may be called from any thread.
 *
 * <pre>{@code
 * try (var client = QueueTiClient.connect("localhost:50051",
 *         ConnectOptions.builder().insecure(true).build())) {
 *     var producer = client.newProducer();
 *     // ...
 * }
 * }</pre>
 */
public final class QueueTiClient implements Closeable {

    private static final Logger logger = Logger.getLogger(QueueTiClient.class.getName());

    /** How many seconds before token expiry to trigger a proactive refresh. */
    private static final long ADVANCE_WINDOW_SECONDS = 60L;
    /** Initial retry backoff in seconds after a refresh failure. */
    private static final long RETRY_BACKOFF_INITIAL_SECONDS = 5L;
    /** Maximum retry backoff in seconds. */
    private static final long RETRY_BACKOFF_MAX_SECONDS = 60L;

    private final ManagedChannel channel;
    private final TokenStore tokenStore;
    private final QueueServiceGrpc.QueueServiceFutureStub futureStub;
    private final QueueServiceGrpc.QueueServiceStub asyncStub;
    private final Thread refresherThread; // nullable

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Package-private constructor used by {@link #connect} and directly by tests via an
     * in-process channel.
     */
    QueueTiClient(
            final ManagedChannel channel,
            final TokenStore tokenStore,
            final QueueServiceGrpc.QueueServiceFutureStub futureStub,
            final QueueServiceGrpc.QueueServiceStub asyncStub,
            final Thread refresherThread) {
        this.channel = channel;
        this.tokenStore = tokenStore;
        this.futureStub = futureStub;
        this.asyncStub = asyncStub;
        this.refresherThread = refresherThread;
    }

    /**
     * Connects to a queue-ti server and returns a fully initialised client.
     *
     * <p>The {@code address} must be in {@code host:port} form. When
     * {@link ConnectOptions#isInsecure()} is {@code true} the channel uses plaintext; otherwise
     * TLS is negotiated automatically.
     *
     * <p>If a {@link TokenRefresher} is configured, a background virtual thread is started
     * immediately and will proactively refresh the token before it expires.
     *
     * @param address the server address in {@code host:port} form; must not be {@code null}.
     *        IPv6 addresses must use bracket notation, e.g. {@code [::1]:50051}.
     * @param options connection configuration; must not be {@code null}
     * @return a connected, ready-to-use {@code QueueTiClient}
     * @throws IllegalArgumentException if {@code address} cannot be parsed as {@code host:port}
     */
    public static QueueTiClient connect(final String address, final ConnectOptions options) {
        final HostPort addr = parseAddress(address);
        final var builder = NettyChannelBuilder.forAddress(addr.host(), addr.port());
        if (options.isInsecure()) {
            builder.usePlaintext();
        } else if (options.getTlsOptions() != null) {
            builder.sslContext(buildSslContext(options.getTlsOptions()));
            final String sni = options.getTlsOptions().getServerNameOverride();
            if (sni != null) {
                builder.overrideAuthority(sni);
            }
        }
        final ManagedChannel channel = builder.build();

        final var tokenStore = new TokenStore(options.getToken());
        final var credentials = new BearerTokenCredentials(tokenStore);

        final var futureStub = QueueServiceGrpc.newFutureStub(channel)
                .withCallCredentials(credentials);
        final var asyncStub = QueueServiceGrpc.newStub(channel)
                .withCallCredentials(credentials);

        Thread refresherThread = null;
        if (options.getTokenRefresher() != null) {
            refresherThread = startRefresher(options.getTokenRefresher(), tokenStore);
        }

        return new QueueTiClient(channel, tokenStore, futureStub, asyncStub, refresherThread);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Replaces the active token. Subsequent RPCs will use the new value immediately.
     *
     * @param token the replacement JWT; may be {@code null} to clear authentication
     */
    public void setToken(final String token) {
        tokenStore.set(token);
    }

    /**
     * Creates a new {@link Producer} backed by this client's channel and credentials.
     *
     * @return a new {@code Producer}; never {@code null}
     */
    public Producer newProducer() {
        return new Producer(futureStub);
    }

    /**
     * Creates a new {@link Consumer} for {@code topic} with default options.
     *
     * @param topic the topic to consume from; must not be {@code null}
     * @return a new {@code Consumer}; never {@code null}
     */
    public Consumer newConsumer(final String topic) {
        return newConsumer(topic, ConsumerOptions.builder().build());
    }

    /**
     * Creates a new {@link Consumer} for {@code topic} with the supplied options.
     *
     * @param topic   the topic to consume from; must not be {@code null}
     * @param options consumer configuration; must not be {@code null}
     * @return a new {@code Consumer}; never {@code null}
     */
    public Consumer newConsumer(final String topic, final ConsumerOptions options) {
        return new Consumer(asyncStub, futureStub, topic, options);
    }

    /**
     * Shuts down the client.
     *
     * <p>Stops the background token-refresher thread (if any), then initiates an orderly shutdown
     * of the underlying gRPC channel, waiting up to 5 seconds for in-flight RPCs to complete.
     * Safe to call more than once.
     */
    @Override
    public void close() {
        if (refresherThread != null) {
            refresherThread.interrupt();
            try {
                refresherThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    // -------------------------------------------------------------------------
    // Package-private accessors (tests only)
    // -------------------------------------------------------------------------

    /**
     * Returns the underlying token store.
     *
     * <p>Package-private; intended for use by tests in the same package only.
     *
     * @return the token store; never {@code null}
     */
    TokenStore tokenStore() {
        return tokenStore;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Starts a virtual thread that proactively refreshes the token before it expires.
     *
     * <p>The thread:
     * <ol>
     *   <li>Parses the expiry of the current token. On failure it backs off and retries.</li>
     *   <li>Sleeps until {@code expiry - ADVANCE_WINDOW_SECONDS} seconds from now.</li>
     *   <li>Calls {@link TokenRefresher#refresh()}, stores the result, and resets the backoff.</li>
     *   <li>On any error it logs, doubles the backoff (capped at
     *       {@value #RETRY_BACKOFF_MAX_SECONDS} s), then retries.</li>
     * </ol>
     *
     * @param refresher the strategy for obtaining a fresh token
     * @return the started virtual thread
     */
    private static Thread startRefresher(final TokenRefresher refresher, final TokenStore tokenStore) {
        return Thread.ofVirtual()
                .name("queue-ti-token-refresher")
                .start(() -> runRefreshLoop(refresher, tokenStore));
    }

    private static void runRefreshLoop(final TokenRefresher refresher, final TokenStore tokenStore) {
        long retryBackoffSeconds = RETRY_BACKOFF_INITIAL_SECONDS;

        while (!Thread.currentThread().isInterrupted()) {

            // --- Parse current token expiry ---
            final String currentToken = tokenStore.get();
            final Instant expiry;
            try {
                expiry = TokenStore.parseExpiry(currentToken);
            } catch (Exception e) {
                logger.log(Level.WARNING,
                        "queue-ti: could not parse token expiry, retrying in {0}s: {1}",
                        new Object[]{retryBackoffSeconds, e.getMessage()});
                if (!sleepSeconds(retryBackoffSeconds)) {
                    return;
                }
                retryBackoffSeconds = Math.min(retryBackoffSeconds * 2, RETRY_BACKOFF_MAX_SECONDS);
                continue;
            }

            // --- Sleep until advance window ---
            final long remainingSeconds =
                    expiry.getEpochSecond() - Instant.now().getEpochSecond() - ADVANCE_WINDOW_SECONDS;
            if (remainingSeconds > 0) {
                retryBackoffSeconds = RETRY_BACKOFF_INITIAL_SECONDS; // healthy token path
                if (!sleepSeconds(remainingSeconds)) {
                    return;
                }
            }

            // --- Refresh ---
            try {
                final String newToken = refresher.refresh().get(30, TimeUnit.SECONDS);
                tokenStore.set(newToken);
                retryBackoffSeconds = RETRY_BACKOFF_INITIAL_SECONDS;
                logger.fine("queue-ti: token refreshed successfully");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | TimeoutException e) {
                final Throwable cause = (e instanceof ExecutionException ee) ? ee.getCause() : e;
                final String causeMsg = cause.getMessage() != null
                        ? cause.getMessage()
                        : cause.getClass().getName();
                logger.log(Level.WARNING,
                        "queue-ti: token refresh failed, retrying in {0}s: {1}",
                        new Object[]{retryBackoffSeconds, causeMsg});
                if (!sleepSeconds(retryBackoffSeconds)) {
                    return;
                }
                retryBackoffSeconds = Math.min(retryBackoffSeconds * 2, RETRY_BACKOFF_MAX_SECONDS);
            }
        }
    }

    /**
     * Sleeps for the given number of seconds.
     *
     * @param seconds how long to sleep
     * @return {@code true} if the sleep completed normally; {@code false} if interrupted
     *         (the interrupt flag is restored before returning)
     */
    private static boolean sleepSeconds(final long seconds) {
        try {
            Thread.sleep(Duration.ofSeconds(seconds));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static SslContext buildSslContext(final TlsOptions tls) {
        try {
            final var sslBuilder = GrpcSslContexts.forClient();
            if (tls.getRootCertificates() != null) {
                sslBuilder.trustManager(new ByteArrayInputStream(tls.getRootCertificates()));
            }
            if (tls.getPrivateKey() != null && tls.getCertificateChain() != null) {
                sslBuilder.keyManager(
                        new ByteArrayInputStream(tls.getCertificateChain()),
                        new ByteArrayInputStream(tls.getPrivateKey()));
            }
            return sslBuilder.build();
        } catch (SSLException e) {
            throw new IllegalArgumentException("failed to build SSL context: " + e.getMessage(), e);
        }
    }

    private record HostPort(String host, int port) {}

    private static HostPort parseAddress(final String address) {
        final int lastColon = address.lastIndexOf(':');
        if (lastColon < 0) {
            throw new IllegalArgumentException(
                    "address must be in host:port form, got: " + address);
        }
        final String host = address.substring(0, lastColon);
        try {
            return new HostPort(host, Integer.parseInt(address.substring(lastColon + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "address port is not a valid integer in: " + address, e);
        }
    }
}
