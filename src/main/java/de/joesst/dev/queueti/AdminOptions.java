package de.joesst.dev.queueti;

import java.time.Duration;

/**
 * Configuration options for {@link AdminClient}.
 *
 * <p>Obtain an instance via {@link #builder()} or use {@link #defaults()} for sensible defaults.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class AdminOptions {

    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final String token;
    private final Duration requestTimeout;

    private AdminOptions(final Builder builder) {
        this.token = builder.token;
        this.requestTimeout = builder.requestTimeout;
    }

    /**
     * Returns a new {@code AdminOptions} with all defaults applied: no token and a 30-second
     * request timeout.
     *
     * @return a default {@code AdminOptions} instance
     */
    public static AdminOptions defaults() {
        return builder().build();
    }

    /**
     * Returns a new {@link Builder} for constructing an {@code AdminOptions}.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the bearer token to send on every request, or {@code null} if none was configured.
     *
     * @return the token, or {@code null}
     */
    public String getToken() {
        return token;
    }

    /**
     * Returns the per-request HTTP timeout.
     *
     * @return the request timeout; never {@code null}
     */
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    /** Builder for {@link AdminOptions}. */
    public static final class Builder {

        private String token = null;
        private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;

        private Builder() {}

        /**
         * Sets the bearer token to include in every request's {@code Authorization} header.
         *
         * @param token the JWT; may be {@code null} to send unauthenticated requests
         * @return this builder
         */
        public Builder token(final String token) {
            this.token = token;
            return this;
        }

        /**
         * Sets the per-request HTTP timeout.
         *
         * @param requestTimeout the timeout; must not be {@code null} or negative
         * @return this builder
         * @throws IllegalArgumentException if {@code requestTimeout} is null, negative, or zero
         */
        public Builder requestTimeout(final Duration requestTimeout) {
            if (requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()) {
                throw new IllegalArgumentException(
                        "requestTimeout must be non-null and positive");
            }
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Builds the {@link AdminOptions}.
         *
         * @return a new immutable {@code AdminOptions}
         */
        public AdminOptions build() {
            return new AdminOptions(this);
        }
    }
}
