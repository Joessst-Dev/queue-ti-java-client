package de.joesst.dev.queueti;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueueTiAuthTest {

    // -------------------------------------------------------------------------
    // In-process HTTP server
    // -------------------------------------------------------------------------

    private static final class FakeAuthServer {

        private final HttpServer server;
        final AtomicReference<String> lastLoginBody = new AtomicReference<>();

        private volatile String statusBody   = "{\"auth_required\":true}";
        private volatile int    loginStatus  = 200;
        private volatile String loginBody    = "{\"token\":\"test-token\"}";

        FakeAuthServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/api/auth/status", exchange -> {
                final var bytes = statusBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream out = exchange.getResponseBody()) { out.write(bytes); }
            });
            server.createContext("/api/auth/login", exchange -> {
                lastLoginBody.set(new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8));
                final var bytes = loginBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(loginStatus, bytes.length);
                try (OutputStream out = exchange.getResponseBody()) { out.write(bytes); }
            });
            server.start();
        }

        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        void stop() { server.stop(0); }
    }

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    private FakeAuthServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = new FakeAuthServer();
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    // -------------------------------------------------------------------------
    // login() — auth required
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("login returns token when auth is required and credentials are valid")
    void login_authRequired_returnsToken() {
        final var auth = QueueTiAuth.login(server.baseUrl(), "admin", "secret");

        assertThat(auth.token()).isEqualTo("test-token");
    }

    @Test
    @DisplayName("login sends credentials in request body")
    void login_sendsCredentialsInBody() {
        QueueTiAuth.login(server.baseUrl(), "admin", "secret");

        assertThat(server.lastLoginBody.get())
                .contains("\"username\":\"admin\"")
                .contains("\"password\":\"secret\"");
    }

    // -------------------------------------------------------------------------
    // login() — auth not required
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("login returns null token when auth is not required")
    void login_authNotRequired_returnsNullToken() {
        server.statusBody = "{\"auth_required\":false}";

        final var auth = QueueTiAuth.login(server.baseUrl(), "admin", "secret");

        assertThat(auth.token()).isNull();
    }

    @Test
    @DisplayName("login does not call /login endpoint when auth is not required")
    void login_authNotRequired_doesNotCallLoginEndpoint() {
        server.statusBody = "{\"auth_required\":false}";

        QueueTiAuth.login(server.baseUrl(), "admin", "secret");

        assertThat(server.lastLoginBody.get()).isNull();
    }

    // -------------------------------------------------------------------------
    // login() — failure cases
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("login throws when server returns non-200 from /login")
    void login_nonOkStatus_throws() {
        server.loginStatus = 401;
        server.loginBody   = "{\"error\":\"invalid credentials\"}";

        assertThatThrownBy(() -> QueueTiAuth.login(server.baseUrl(), "wrong", "creds"))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("login failed (HTTP 401)");
    }

    @Test
    @DisplayName("login throws when response body has no token field")
    void login_missingTokenField_throws() {
        server.loginBody = "{\"message\":\"ok\"}";

        assertThatThrownBy(() -> QueueTiAuth.login(server.baseUrl(), "admin", "secret"))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("no token field");
    }

    // -------------------------------------------------------------------------
    // refresh()
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("refresh re-authenticates and returns a new token")
    void refresh_returnsNewToken() throws Exception {
        final var auth = QueueTiAuth.login(server.baseUrl(), "admin", "secret");

        server.loginBody = "{\"token\":\"refreshed-token\"}";
        final var newToken = auth.refresh().get();

        assertThat(newToken).isEqualTo("refreshed-token");
        assertThat(auth.token()).isEqualTo("refreshed-token");
    }

    @Test
    @DisplayName("refresh updates the stored token")
    void refresh_updatesStoredToken() throws Exception {
        final var auth = QueueTiAuth.login(server.baseUrl(), "admin", "secret");
        assertThat(auth.token()).isEqualTo("test-token");

        server.loginBody = "{\"token\":\"second-token\"}";
        auth.refresh().get();

        assertThat(auth.token()).isEqualTo("second-token");
    }

    @Test
    @DisplayName("refresh is a no-op when auth is not required")
    void refresh_authNotRequired_noOp() throws Exception {
        server.statusBody = "{\"auth_required\":false}";
        final var auth = QueueTiAuth.login(server.baseUrl(), "admin", "secret");

        final var result = auth.refresh().get();

        assertThat(result).isNull();
        assertThat(auth.token()).isNull();
    }

    @Test
    @DisplayName("refresh completes exceptionally when server returns non-200")
    void refresh_serverError_completesExceptionally() {
        final var auth = QueueTiAuth.login(server.baseUrl(), "admin", "secret");
        server.loginStatus = 401;
        server.loginBody   = "{\"error\":\"invalid credentials\"}";

        assertThatThrownBy(() -> auth.refresh().get())
                .cause()
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("login failed (HTTP 401)");
    }

    // -------------------------------------------------------------------------
    // Tolerates trailing slash in adminAddr
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("login normalises trailing slash in adminAddr")
    void login_trailingSlash_normalised() {
        final var auth = QueueTiAuth.login(server.baseUrl() + "/", "admin", "secret");

        assertThat(auth.token()).isEqualTo("test-token");
    }
}
