package de.joesst.dev.queueti;

/**
 * Configuration options for establishing a connection to the queue-ti server.
 *
 * <p>Obtain an instance via {@link #builder()} or use {@link #defaults()} for a
 * zero-configuration default.
 *
 * <p>Instances are immutable and thread-safe.
 */
public final class ConnectOptions {

    private final boolean insecure;
    private final String token;
    private final TokenRefresher tokenRefresher;

    private ConnectOptions(Builder builder) {
        this.insecure = builder.insecure;
        this.token = builder.token;
        this.tokenRefresher = builder.tokenRefresher;
    }

    /**
     * Returns a new {@code ConnectOptions} with all defaults applied:
     * {@code insecure=false}, no static token, and no token refresher.
     *
     * @return a default {@code ConnectOptions} instance
     */
    public static ConnectOptions defaults() {
        return builder().build();
    }

    /**
     * Returns a new {@link Builder} for constructing a {@code ConnectOptions}.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns {@code true} if TLS verification should be disabled.
     *
     * @return {@code true} for a plaintext / insecure channel
     */
    public boolean isInsecure() {
        return insecure;
    }

    /**
     * Returns the static JWT to send on every request, or {@code null} if none was set.
     *
     * @return the static token, or {@code null}
     */
    public String getToken() {
        return token;
    }

    /**
     * Returns the {@link TokenRefresher} used to obtain fresh tokens, or {@code null} if none
     * was configured.
     *
     * @return the token refresher, or {@code null}
     */
    public TokenRefresher getTokenRefresher() {
        return tokenRefresher;
    }

    /** Builder for {@link ConnectOptions}. */
    public static final class Builder {

        private boolean insecure = false;
        private String token = null;
        private TokenRefresher tokenRefresher = null;

        private Builder() {}

        /**
         * Disables TLS verification when set to {@code true}.
         *
         * @param insecure {@code true} to use a plaintext channel
         * @return this builder
         */
        public Builder insecure(boolean insecure) {
            this.insecure = insecure;
            return this;
        }

        /**
         * Sets a static JWT to send on every request.
         *
         * @param token the static token; may be {@code null}
         * @return this builder
         */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Sets a {@link TokenRefresher} for dynamic token renewal.
         *
         * @param tokenRefresher the refresher; may be {@code null}
         * @return this builder
         */
        public Builder tokenRefresher(TokenRefresher tokenRefresher) {
            this.tokenRefresher = tokenRefresher;
            return this;
        }

        /**
         * Builds the {@link ConnectOptions}.
         *
         * @return a new immutable {@code ConnectOptions}
         */
        public ConnectOptions build() {
            return new ConnectOptions(this);
        }
    }
}
