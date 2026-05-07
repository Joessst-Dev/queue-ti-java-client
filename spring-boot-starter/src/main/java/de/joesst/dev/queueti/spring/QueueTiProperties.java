package de.joesst.dev.queueti.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;

import java.time.Duration;

/**
 * Configuration properties for the queue-ti Spring Boot starter.
 *
 * <p>All properties are under the {@code queueti} prefix. Example:
 *
 * <pre>{@code
 * queueti:
 *   grpc-address: localhost:50051
 *   insecure: true
 *   admin:
 *     url: http://localhost:8080
 *   auth:
 *     admin-address: http://localhost:8080
 *     username: admin
 *     password: secret
 * }</pre>
 */
@ConfigurationProperties(prefix = "queueti")
public class QueueTiProperties {

    /** gRPC server address in {@code host:port} form. Required. */
    private String grpcAddress;

    /** Use a plaintext (insecure) channel. Mutually exclusive with {@code tls.*}. */
    private boolean insecure = false;

    /** Static JWT sent on every request. Superseded by {@code auth.*} when both are set. */
    private String token;

    private TlsProperties tls = new TlsProperties();
    private AuthProperties auth = new AuthProperties();
    private AdminProperties admin = new AdminProperties();

    public String getGrpcAddress() { return grpcAddress; }
    public void setGrpcAddress(String grpcAddress) { this.grpcAddress = grpcAddress; }

    public boolean isInsecure() { return insecure; }
    public void setInsecure(boolean insecure) { this.insecure = insecure; }

    public String getToken() { return token; }
    public void setToken(String token) { this.token = token; }

    public TlsProperties getTls() { return tls; }
    public void setTls(TlsProperties tls) { this.tls = tls; }

    public AuthProperties getAuth() { return auth; }
    public void setAuth(AuthProperties auth) { this.auth = auth; }

    public AdminProperties getAdmin() { return admin; }
    public void setAdmin(AdminProperties admin) { this.admin = admin; }

    // -------------------------------------------------------------------------
    // Nested: TLS
    // -------------------------------------------------------------------------

    /**
     * TLS configuration. Maps to the {@code queueti.tls.*} properties.
     *
     * <p>File paths are resolved as Spring {@link Resource}s, supporting {@code classpath:},
     * {@code file:}, and bare filesystem paths (including {@code ${ENV_VAR}/ca.pem}).
     */
    public static class TlsProperties {

        /** PEM CA certificate(s). {@code null} uses the JVM default trust store. */
        private Resource rootCertificates;

        /** PEM client private key for mTLS. Must be paired with {@code certificateChain}. */
        private Resource privateKey;

        /** PEM client certificate chain for mTLS. Must be paired with {@code privateKey}. */
        private Resource certificateChain;

        /** Override hostname for SNI and certificate verification. */
        private String serverNameOverride;

        public Resource getRootCertificates() { return rootCertificates; }
        public void setRootCertificates(Resource rootCertificates) { this.rootCertificates = rootCertificates; }

        public Resource getPrivateKey() { return privateKey; }
        public void setPrivateKey(Resource privateKey) { this.privateKey = privateKey; }

        public Resource getCertificateChain() { return certificateChain; }
        public void setCertificateChain(Resource certificateChain) { this.certificateChain = certificateChain; }

        public String getServerNameOverride() { return serverNameOverride; }
        public void setServerNameOverride(String serverNameOverride) { this.serverNameOverride = serverNameOverride; }

        /** Returns {@code true} if any TLS field has been set. */
        public boolean hasAnyValue() {
            return rootCertificates != null
                    || privateKey != null
                    || certificateChain != null
                    || serverNameOverride != null;
        }
    }

    // -------------------------------------------------------------------------
    // Nested: Auth
    // -------------------------------------------------------------------------

    /**
     * Credential-based auth configuration. Maps to {@code queueti.auth.*}.
     *
     * <p>When {@code adminAddress} is set, a {@link de.joesst.dev.queueti.QueueTiAuth} bean is
     * created that checks {@code /api/auth/status} and logs in if required. The resulting JWT
     * is wired into both the gRPC client and the admin client automatically.
     */
    public static class AuthProperties {

        /** HTTP base URL of the admin API used for login. Activates the {@code QueueTiAuth} bean. */
        private String adminAddress;

        private String username;
        private String password;

        public String getAdminAddress() { return adminAddress; }
        public void setAdminAddress(String adminAddress) { this.adminAddress = adminAddress; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }

    // -------------------------------------------------------------------------
    // Nested: Admin
    // -------------------------------------------------------------------------

    /**
     * Admin HTTP client configuration. Maps to {@code queueti.admin.*}.
     *
     * <p>When {@code url} is set, an {@link de.joesst.dev.queueti.AdminClient} bean is created.
     */
    public static class AdminProperties {

        /** HTTP base URL of the admin API. Activates the {@code AdminClient} bean. */
        private String url;

        /** Per-request HTTP timeout. Accepts {@code 30s}, {@code PT30S}, {@code 1m}. */
        private Duration requestTimeout = Duration.ofSeconds(30);

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }

        public Duration getRequestTimeout() { return requestTimeout; }
        public void setRequestTimeout(Duration requestTimeout) { this.requestTimeout = requestTimeout; }
    }
}
