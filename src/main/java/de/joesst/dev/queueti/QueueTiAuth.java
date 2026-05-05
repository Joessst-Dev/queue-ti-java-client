package de.joesst.dev.queueti;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

/**
 * Handles username/password authentication against the queue-ti HTTP admin API.
 *
 * <p>Obtain an instance via {@link #login(String, String, String)}:
 *
 * <pre>{@code
 * QueueTiAuth auth = QueueTiAuth.login("http://localhost:8080", "admin", "secret");
 *
 * try (var client = QueueTiClient.connect("localhost:50051",
 *         ConnectOptions.builder()
 *                 .insecure(true)
 *                 .token(auth.token())
 *                 .tokenRefresher(auth)
 *                 .build())) {
 *     // ...
 * }
 * }</pre>
 *
 * <p>If the server has authentication disabled, {@link #login} returns an instance where
 * {@link #token()} is {@code null} and the refresher is a no-op.
 *
 * <p>Implements {@link TokenRefresher} so the same instance can be passed directly to
 * {@link ConnectOptions.Builder#tokenRefresher(TokenRefresher)}.
 */
public final class QueueTiAuth implements TokenRefresher {

    private final String adminAddr;
    private final String username;
    private final String password;
    private volatile String token;

    private QueueTiAuth(final String adminAddr, final String username, final String password,
            final String token) {
        this.adminAddr = adminAddr;
        this.username = username;
        this.password = password;
        this.token = token;
    }

    /**
     * Checks whether the server requires authentication and, if so, logs in with the supplied
     * credentials to obtain a JWT.
     *
     * <p>If the server does not require authentication, the returned instance has a {@code null}
     * token and a no-op refresher.
     *
     * @param adminAddr the base URL of the queue-ti admin API (e.g. {@code http://localhost:8080})
     * @param username  login username
     * @param password  login password
     * @return an authenticated {@code QueueTiAuth} instance
     * @throws UncheckedIOException if the HTTP request fails or login returns a non-200 status
     */
    public static QueueTiAuth login(final String adminAddr, final String username,
            final String password) {
        final var normalised = adminAddr.endsWith("/")
                ? adminAddr.substring(0, adminAddr.length() - 1)
                : adminAddr;
        final var http = HttpClient.newHttpClient();
        final var token = doLogin(http, normalised, username, password);
        return new QueueTiAuth(normalised, username, password, token);
    }

    /**
     * Returns the current JWT, or {@code null} if the server does not require authentication.
     *
     * @return the current JWT, or {@code null}
     */
    public String token() {
        return token;
    }

    /**
     * Re-authenticates with the server and returns a fresh JWT.
     *
     * <p>Called automatically by {@link QueueTiClient}'s background token refresher before the
     * current token expires.
     *
     * @return a future that completes with the new JWT; never completes with {@code null}
     * @throws UncheckedIOException if re-authentication fails
     */
    @Override
    public CompletableFuture<String> refresh() {
        return CompletableFuture.supplyAsync(() -> {
            final var http = HttpClient.newHttpClient();
            final var newToken = doLogin(http, adminAddr, username, password);
            if (newToken != null) {
                token = newToken;
            }
            return newToken;
        });
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static String doLogin(final HttpClient http, final String adminAddr,
            final String username, final String password) {
        if (!isAuthRequired(http, adminAddr)) {
            return null;
        }
        return fetchToken(http, adminAddr, username, password);
    }

    private static boolean isAuthRequired(final HttpClient http, final String adminAddr) {
        final var request = HttpRequest.newBuilder()
                .uri(URI.create(adminAddr + "/api/auth/status"))
                .GET()
                .build();
        try {
            final var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body().contains("\"auth_required\":true");
        } catch (IOException e) {
            throw new UncheckedIOException("auth status check failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException("auth status check interrupted",
                    new IOException(e.getMessage(), e));
        }
    }

    private static String fetchToken(final HttpClient http, final String adminAddr,
            final String username, final String password) {
        final var body = "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}";
        final var request = HttpRequest.newBuilder()
                .uri(URI.create(adminAddr + "/api/auth/login"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();
        final HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException("login request failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException("login request interrupted",
                    new IOException(e.getMessage(), e));
        }
        if (response.statusCode() != 200) {
            throw new UncheckedIOException(
                    "login failed (HTTP " + response.statusCode() + "): " + response.body(),
                    new IOException("unexpected HTTP status " + response.statusCode()));
        }
        return extractToken(response.body());
    }

    private static String extractToken(final String responseBody) {
        final var prefix = "\"token\":\"";
        final var start = responseBody.indexOf(prefix);
        if (start < 0) {
            throw new UncheckedIOException("unexpected login response — no token field",
                    new IOException("missing token field in: " + responseBody));
        }
        final var tokenStart = start + prefix.length();
        final var end = responseBody.indexOf("\"", tokenStart);
        if (end < 0) {
            throw new UncheckedIOException("unexpected login response — unterminated token",
                    new IOException("unterminated token in: " + responseBody));
        }
        return responseBody.substring(tokenStart, end);
    }
}
