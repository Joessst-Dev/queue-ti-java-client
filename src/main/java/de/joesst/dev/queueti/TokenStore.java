package de.joesst.dev.queueti;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Thread-safe store for a JWT access token, guarded by a {@link ReentrantReadWriteLock}.
 * <p>
 * Package-private: consumers interact with the token lifecycle through {@code ConnectOptions}
 * and {@code TokenRefresher}, not directly through this class.
 */
class TokenStore {

    private static final Pattern EXP_PATTERN = Pattern.compile("\"exp\"\\s*:\\s*(\\d+)");

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private String token;

    /**
     * Constructs a new {@code TokenStore} with the given initial token.
     *
     * @param initialToken the starting token, or {@code null} if none is available yet
     */
    TokenStore(String initialToken) {
        this.token = initialToken;
    }

    /**
     * Returns the current token, acquiring a read lock for safe concurrent access.
     *
     * @return the current token, or {@code null} if none has been set
     */
    String get() {
        final var readLock = lock.readLock();
        readLock.lock();
        try {
            return token;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Replaces the current token, acquiring a write lock for safe concurrent access.
     *
     * @param newToken the replacement token; may be {@code null}
     */
    void set(String newToken) {
        final var writeLock = lock.writeLock();
        writeLock.lock();
        try {
            this.token = newToken;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Parses the {@code exp} claim from the payload segment of a JWT and returns it as an
     * {@link Instant}.
     *
     * <p>The method:
     * <ol>
     *   <li>Splits the JWT on {@code '.'} — exactly three segments are required.</li>
     *   <li>Base64URL-decodes the middle (payload) segment, padding to a multiple of 4 first.</li>
     *   <li>Locates {@code "exp":<digits>} in the decoded JSON string.</li>
     *   <li>Returns {@code Instant.ofEpochSecond(exp)}.</li>
     * </ol>
     *
     * @param jwt a compact JWT string (header.payload.signature)
     * @return the expiry instant encoded in the {@code exp} claim
     * @throws IllegalArgumentException if the JWT does not have exactly three dot-separated
     *         segments, or if the payload contains no {@code exp} claim
     */
    static Instant parseExpiry(String jwt) {
        final String[] parts = jwt.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "JWT must have exactly 3 dot-separated segments, got " + parts.length);
        }

        final String paddedPayload = padBase64(parts[1]);
        final byte[] decoded = Base64.getUrlDecoder().decode(paddedPayload);
        final String payloadJson = new String(decoded, StandardCharsets.UTF_8);

        final Matcher matcher = EXP_PATTERN.matcher(payloadJson);
        if (!matcher.find()) {
            throw new IllegalArgumentException("JWT has no exp claim");
        }

        return Instant.ofEpochSecond(Long.parseLong(matcher.group(1)));
    }

    /**
     * Pads a Base64URL-encoded string to the nearest multiple of 4 with {@code '='} characters,
     * as required by {@link Base64#getUrlDecoder()}.
     */
    private static String padBase64(String base64) {
        final int remainder = base64.length() % 4;
        if (remainder == 0) {
            return base64;
        }
        return base64 + "=".repeat(4 - remainder);
    }
}
