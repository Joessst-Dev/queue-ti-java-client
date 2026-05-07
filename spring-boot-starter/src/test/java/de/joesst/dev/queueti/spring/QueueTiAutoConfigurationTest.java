package de.joesst.dev.queueti.spring;

import com.sun.net.httpserver.HttpServer;
import de.joesst.dev.queueti.AdminClient;
import de.joesst.dev.queueti.Producer;
import de.joesst.dev.queueti.QueueTiAuth;
import de.joesst.dev.queueti.QueueTiClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class QueueTiAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(QueueTiAutoConfiguration.class));

    // -------------------------------------------------------------------------
    // Fake auth server (for tests that need QueueTiAuth)
    // -------------------------------------------------------------------------

    private HttpServer fakeAuthServer;
    private int fakeAuthPort;

    @BeforeEach
    void startFakeAuthServer() throws IOException {
        fakeAuthServer = HttpServer.create(new InetSocketAddress(0), 0);
        fakeAuthServer.createContext("/api/auth/status", exchange -> {
            final var body = "{\"auth_required\":true}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
        });
        fakeAuthServer.createContext("/api/auth/login", exchange -> {
            final var body = "{\"token\":\"test-jwt\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
        });
        fakeAuthServer.start();
        fakeAuthPort = fakeAuthServer.getAddress().getPort();
    }

    @AfterEach
    void stopFakeAuthServer() {
        fakeAuthServer.stop(0);
    }

    // -------------------------------------------------------------------------
    // 1. Minimal config
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("minimal config creates QueueTiClient and Producer; no AdminClient or QueueTiAuth")
    void minimalConfig_createsClientAndProducer() {
        runner.withPropertyValues(
                        "queueti.grpc-address=localhost:50051",
                        "queueti.insecure=true")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(QueueTiClient.class);
                    assertThat(ctx).hasSingleBean(Producer.class);
                    assertThat(ctx).doesNotHaveBean(AdminClient.class);
                    assertThat(ctx).doesNotHaveBean(QueueTiAuth.class);
                });
    }

    // -------------------------------------------------------------------------
    // 2. No grpc-address → no beans at all
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("no grpc-address → context contains no QueueTi beans")
    void noGrpcAddress_noBeans() {
        runner.run(ctx -> {
            assertThat(ctx).doesNotHaveBean(QueueTiClient.class);
            assertThat(ctx).doesNotHaveBean(Producer.class);
            assertThat(ctx).doesNotHaveBean(AdminClient.class);
            assertThat(ctx).doesNotHaveBean(QueueTiAuth.class);
        });
    }

    // -------------------------------------------------------------------------
    // 3. Admin URL → AdminClient bean appears
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("queueti.admin.url activates AdminClient bean")
    void adminUrl_activatesAdminClient() {
        runner.withPropertyValues(
                        "queueti.grpc-address=localhost:50051",
                        "queueti.insecure=true",
                        "queueti.admin.url=http://localhost:8080")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(AdminClient.class);
                });
    }

    // -------------------------------------------------------------------------
    // 4. Auth config → QueueTiAuth bean appears
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("queueti.auth.admin-address activates QueueTiAuth bean")
    void authConfig_activatesQueueTiAuth() {
        runner.withPropertyValues(
                        "queueti.grpc-address=localhost:50051",
                        "queueti.insecure=true",
                        "queueti.auth.admin-address=http://localhost:" + fakeAuthPort,
                        "queueti.auth.username=admin",
                        "queueti.auth.password=secret")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(QueueTiAuth.class);
                    final var auth = ctx.getBean(QueueTiAuth.class);
                    assertThat(auth.token()).isEqualTo("test-jwt");
                });
    }

    // -------------------------------------------------------------------------
    // 5. TLS resources → client created without error
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("queueti.tls.root-certificates creates QueueTiClient with custom CA")
    void tlsRootCertificates_createsClient() {
        runner.withPropertyValues(
                        "queueti.grpc-address=localhost:50051",
                        "queueti.tls.root-certificates=classpath:test-ca.pem")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(QueueTiClient.class);
                });
    }

    @Test
    @DisplayName("queueti.tls.* mTLS properties create QueueTiClient")
    void tlsMtls_createsClient() {
        runner.withPropertyValues(
                        "queueti.grpc-address=localhost:50051",
                        "queueti.tls.root-certificates=classpath:test-ca.pem",
                        "queueti.tls.private-key=classpath:test-client-key.pem",
                        "queueti.tls.certificate-chain=classpath:test-client-cert.pem")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(QueueTiClient.class);
                });
    }

    // -------------------------------------------------------------------------
    // 6. @ConditionalOnMissingBean — user override backs off auto-config
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("user-defined QueueTiClient bean prevents auto-configuration")
    void userDefinedClient_autoConfigBacksOff() {
        runner.withPropertyValues(
                        "queueti.grpc-address=localhost:50051",
                        "queueti.insecure=true")
                .withUserConfiguration(UserClientConfig.class)
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(QueueTiClient.class);
                    assertThat(ctx.getBean(QueueTiClient.class))
                            .isSameAs(ctx.getBean("customClient"));
                });
    }

    @Configuration
    static class UserClientConfig {
        @Bean(name = "customClient")
        QueueTiClient customClient() {
            return QueueTiClient.connect("localhost:50051",
                    de.joesst.dev.queueti.ConnectOptions.builder().insecure(true).build());
        }
    }
}
