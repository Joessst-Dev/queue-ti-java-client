package de.joesst.dev.queueti;

/**
 * TLS configuration for a {@link QueueTiClient} connection.
 *
 * <p>All fields are optional:
 * <ul>
 *   <li>{@link #getRootCertificates()} — PEM-encoded CA certificate(s) to trust instead of
 *       the JVM's default trust store. Use for self-signed or internal CA setups.</li>
 *   <li>{@link #getPrivateKey()} + {@link #getCertificateChain()} — PEM-encoded client key and
 *       certificate chain for mutual TLS (mTLS).</li>
 *   <li>{@link #getServerNameOverride()} — overrides the hostname used for SNI and certificate
 *       verification. Useful when the server's certificate does not match the dial address.</li>
 * </ul>
 *
 * <p>When no {@code TlsOptions} is supplied to
 * {@link ConnectOptions.Builder#tls(TlsOptions)}, the client uses TLS with the JVM's default
 * trust store (system CAs).
 *
 * <pre>{@code
 * // Custom CA
 * byte[] ca = Files.readAllBytes(Path.of("ca.pem"));
 * TlsOptions tls = TlsOptions.builder().rootCertificates(ca).build();
 *
 * // mTLS
 * TlsOptions tls = TlsOptions.builder()
 *         .rootCertificates(ca)
 *         .privateKey(Files.readAllBytes(Path.of("client-key.pem")))
 *         .certificateChain(Files.readAllBytes(Path.of("client-cert.pem")))
 *         .build();
 *
 * // Server name override
 * TlsOptions tls = TlsOptions.builder()
 *         .rootCertificates(ca)
 *         .serverNameOverride("myserver.internal")
 *         .build();
 * }</pre>
 *
 * <p>Instances are immutable. The byte arrays passed to the builder are defensively copied.
 */
public final class TlsOptions {

    private final byte[] rootCertificates;
    private final byte[] privateKey;
    private final byte[] certificateChain;
    private final String serverNameOverride;

    private TlsOptions(final Builder builder) {
        this.rootCertificates  = copy(builder.rootCertificates);
        this.privateKey        = copy(builder.privateKey);
        this.certificateChain  = copy(builder.certificateChain);
        this.serverNameOverride = builder.serverNameOverride;
    }

    /** Returns a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the PEM-encoded CA certificate(s), or {@code null} to use the JVM default
     * trust store.
     *
     * @return defensive copy of the PEM bytes, or {@code null}
     */
    public byte[] getRootCertificates() {
        return copy(rootCertificates);
    }

    /**
     * Returns the PEM-encoded client private key for mTLS, or {@code null} if mTLS is not
     * configured.
     *
     * @return defensive copy of the PEM bytes, or {@code null}
     */
    public byte[] getPrivateKey() {
        return copy(privateKey);
    }

    /**
     * Returns the PEM-encoded client certificate chain for mTLS, or {@code null} if mTLS is not
     * configured.
     *
     * @return defensive copy of the PEM bytes, or {@code null}
     */
    public byte[] getCertificateChain() {
        return copy(certificateChain);
    }

    /**
     * Returns the server name override for SNI and hostname verification, or {@code null} to
     * use the dial address.
     *
     * @return the override, or {@code null}
     */
    public String getServerNameOverride() {
        return serverNameOverride;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    /** Builder for {@link TlsOptions}. */
    public static final class Builder {

        private byte[] rootCertificates;
        private byte[] privateKey;
        private byte[] certificateChain;
        private String serverNameOverride;

        private Builder() {}

        /**
         * Sets PEM-encoded CA certificate(s) to use as the trust anchor instead of the JVM
         * default trust store.
         *
         * @param pem PEM bytes; may be {@code null} to use system CAs
         * @return this builder
         */
        public Builder rootCertificates(final byte[] pem) {
            this.rootCertificates = copy(pem);
            return this;
        }

        /**
         * Sets the PEM-encoded client private key for mutual TLS. Must be paired with
         * {@link #certificateChain(byte[])}.
         *
         * @param pem PEM bytes; may be {@code null}
         * @return this builder
         */
        public Builder privateKey(final byte[] pem) {
            this.privateKey = copy(pem);
            return this;
        }

        /**
         * Sets the PEM-encoded client certificate chain for mutual TLS. Must be paired with
         * {@link #privateKey(byte[])}.
         *
         * @param pem PEM bytes; may be {@code null}
         * @return this builder
         */
        public Builder certificateChain(final byte[] pem) {
            this.certificateChain = copy(pem);
            return this;
        }

        /**
         * Overrides the hostname used for SNI and certificate verification.
         *
         * @param name the server name; may be {@code null}
         * @return this builder
         */
        public Builder serverNameOverride(final String name) {
            this.serverNameOverride = name;
            return this;
        }

        /**
         * Builds the {@link TlsOptions}.
         *
         * @return a new immutable {@code TlsOptions} instance
         * @throws IllegalArgumentException if only one of {@code privateKey} /
         *         {@code certificateChain} is set (mTLS requires both)
         */
        public TlsOptions build() {
            final boolean hasKey  = privateKey != null;
            final boolean hasCert = certificateChain != null;
            if (hasKey != hasCert) {
                throw new IllegalArgumentException(
                        "privateKey and certificateChain must both be set for mTLS, or both null");
            }
            return new TlsOptions(this);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] copy(final byte[] src) {
        return src == null ? null : src.clone();
    }
}
