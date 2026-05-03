package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class QueueTiClientTest {

    // -------------------------------------------------------------------------
    // In-process gRPC server infrastructure
    // -------------------------------------------------------------------------

    /** No-op service implementation — just enough for the channel to be wired. */
    private static final class FakeQueueService extends QueueServiceGrpc.QueueServiceImplBase {
        // All RPC methods fall through to the default UNIMPLEMENTED response,
        // which is fine because no test actually invokes an RPC.
    }

    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private String serverName;

    @BeforeEach
    void startInProcessServer() throws IOException {
        serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(new FakeQueueService())
                .build()
                .start();
        inProcessChannel = InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build();
    }

    @AfterEach
    void stopInProcessServer() throws InterruptedException {
        inProcessChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        inProcessServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link QueueTiClient} wired to the in-process channel with the given initial token
     * and no refresher thread.
     */
    private QueueTiClient buildClient(final String initialToken) {
        final var tokenStore = new TokenStore(initialToken);
        final var credentials = new BearerTokenCredentials(tokenStore);
        final var futureStub = QueueServiceGrpc.newFutureStub(inProcessChannel)
                .withCallCredentials(credentials);
        final var asyncStub = QueueServiceGrpc.newStub(inProcessChannel)
                .withCallCredentials(credentials);
        return new QueueTiClient(inProcessChannel, tokenStore, futureStub, asyncStub, null);
    }

    /**
     * Creates a compact JWT whose {@code exp} claim is {@code epochSecond} seconds since the
     * Unix epoch. The header and signature segments are placeholders — only the payload matters
     * for {@link TokenStore#parseExpiry}.
     */
    private static String jwtWithExp(final long epochSecond) {
        final String payload = "{\"exp\":" + epochSecond + "}";
        final String encodedPayload = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return "eyJhbGciOiJIUzI1NiJ9." + encodedPayload + ".signature";
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("connect builds a client with an insecure channel without throwing")
    void connect_builds_client_with_insecure_channel() {
        // Given
        final var options = ConnectOptions.builder().insecure(true).build();

        // When / Then
        assertThatNoException().isThrownBy(() -> {
            try (var client = QueueTiClient.connect("localhost:50051", options)) {
                assertThat(client).isNotNull();
            }
        });
    }

    @Test
    @DisplayName("setToken updates the value readable from the token store")
    void setToken_updates_token_store() {
        // Given
        final var client = buildClient("initial-token");

        // When
        client.setToken("newtoken");

        // Then
        assertThat(client.tokenStore().get()).isEqualTo("newtoken");
    }

    @Test
    @DisplayName("newProducer returns a non-null Producer")
    void newProducer_returns_non_null_producer() {
        // Given
        final var client = buildClient(null);

        // When
        final var producer = client.newProducer();

        // Then
        assertThat(producer).isNotNull();
    }

    @Test
    @DisplayName("newConsumer returns a non-null Consumer")
    void newConsumer_returns_non_null_consumer() {
        // Given
        final var client = buildClient(null);

        // When
        final var consumer = client.newConsumer("my-topic");

        // Then
        assertThat(consumer).isNotNull();
    }

    @Test
    @DisplayName("close can be called twice without throwing")
    void close_is_idempotent() throws Exception {
        // Given
        final var client = buildClient(null);

        // When / Then — second close must not throw even though channel is already shut down
        assertThatNoException().isThrownBy(() -> {
            client.close();
            client.close();
        });
    }

    @Test
    @DisplayName("refresher thread is no longer alive after close")
    void refresher_thread_is_stopped_after_close() throws Exception {
        // Given — a client constructed with a refresher that never actually fires
        // (the token expiry is far in the future so the refresher thread just sleeps)
        final long farFuture = Instant.now().getEpochSecond() + 3600;
        final var tokenStore = new TokenStore(jwtWithExp(farFuture));
        final var credentials = new BearerTokenCredentials(tokenStore);
        final var futureStub = QueueServiceGrpc.newFutureStub(inProcessChannel)
                .withCallCredentials(credentials);
        final var asyncStub = QueueServiceGrpc.newStub(inProcessChannel)
                .withCallCredentials(credentials);

        // Start a real refresher thread using the package-private constructor trick:
        // build a temporary client, extract its thread, then wrap it in the real client.
        final var refreshedToken = new AtomicReference<String>("unchanged");
        final TokenRefresher noopRefresher =
                () -> CompletableFuture.completedFuture("refreshed");

        // Use connect() on in-process won't work (NettyChannelBuilder), so wire the thread
        // manually via the package-private constructor path.
        final var tempClient = new QueueTiClient(
                inProcessChannel, tokenStore, futureStub, asyncStub, null);

        // Reflectively start the refresher via the private method isn't possible cleanly;
        // instead we spin a virtual thread that mimics what QueueTiClient.startRefresher does
        // (just blocks indefinitely) so we can assert it stops on close().
        final var latchedThread = Thread.ofVirtual()
                .name("queue-ti-token-refresher")
                .start(() -> {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        final var client = new QueueTiClient(
                inProcessChannel, tokenStore, futureStub, asyncStub, latchedThread);

        // When
        client.close();

        // Then
        assertThat(latchedThread.isAlive()).isFalse();
    }

    @Test
    @DisplayName("refresher calls TokenRefresher and stores the new token when token is near expiry")
    void refresher_calls_token_refresher_when_token_near_expiry() throws Exception {
        // Given — a JWT whose exp is 30 seconds from now (within the 60 s advance window)
        final long expSoon = Instant.now().getEpochSecond() + 30;
        final String nearExpiryJwt = jwtWithExp(expSoon);

        final var options = ConnectOptions.builder()
                .insecure(true)
                .token(nearExpiryJwt)
                .tokenRefresher(() -> CompletableFuture.completedFuture("refreshed-token-value"))
                .build();

        // When — connect starts the refresher thread automatically
        try (var client = QueueTiClient.connect("localhost:50051", options)) {

            // Then — poll up to 3 s for the token store to reflect the refreshed value
            final long deadline = System.currentTimeMillis() + 3_000;
            String observed = null;
            while (System.currentTimeMillis() < deadline) {
                observed = client.tokenStore().get();
                if ("refreshed-token-value".equals(observed)) {
                    break;
                }
                //noinspection BusyWait
                Thread.sleep(50);
            }

            assertThat(observed).isEqualTo("refreshed-token-value");
        }
    }
}
