package de.joesst.dev.queueti.spring;

import de.joesst.dev.queueti.AdminClient;
import de.joesst.dev.queueti.AdminOptions;
import de.joesst.dev.queueti.ConnectOptions;
import de.joesst.dev.queueti.Producer;
import de.joesst.dev.queueti.QueueTiAuth;
import de.joesst.dev.queueti.QueueTiClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

/**
 * Spring Boot auto-configuration for the queue-ti Java client.
 *
 * <p>Activated when {@code queueti.grpc-address} is present on the classpath. Creates the
 * following beans depending on which properties are configured:
 *
 * <ul>
 *   <li>{@link QueueTiClient} — always (when {@code queueti.grpc-address} is set)</li>
 *   <li>{@link Producer} — always (derived from the client)</li>
 *   <li>{@link QueueTiAuth} — when {@code queueti.auth.admin-address} is set</li>
 *   <li>{@link AdminClient} — when {@code queueti.admin.url} is set</li>
 * </ul>
 *
 * <p>All beans support {@code @ConditionalOnMissingBean} — define your own {@code @Bean} to
 * override any of them.
 *
 * <p>{@link QueueTiClient} implements {@link java.io.Closeable}; Spring Framework 6.x calls
 * {@code close()} automatically during context shutdown.
 */
@AutoConfiguration
@EnableConfigurationProperties(QueueTiProperties.class)
@ConditionalOnClass(QueueTiClient.class)
@ConditionalOnProperty(prefix = "queueti", name = "grpc-address")
public class QueueTiAutoConfiguration {

    // -------------------------------------------------------------------------
    // QueueTiAuth — optional, activated by queueti.auth.admin-address
    // -------------------------------------------------------------------------

    @Bean
    @ConditionalOnProperty(prefix = "queueti.auth", name = "admin-address")
    public QueueTiAuth queueTiAuth(final QueueTiProperties props) {
        final var auth = props.getAuth();
        return QueueTiAuth.login(auth.getAdminAddress(), auth.getUsername(), auth.getPassword());
    }

    // -------------------------------------------------------------------------
    // QueueTiClient
    // -------------------------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean(QueueTiClient.class)
    public QueueTiClient queueTiClient(
            final QueueTiProperties props,
            @Autowired(required = false) final QueueTiAuth queueTiAuth) {

        final var optBuilder = ConnectOptions.builder();

        if (props.isInsecure()) {
            optBuilder.insecure(true);
        } else if (props.getTls().hasAnyValue()) {
            try {
                optBuilder.tls(QueueTiTlsResourceLoader.toTlsOptions(props.getTls()));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load TLS resources for queueTiClient", e);
            }
        }

        if (queueTiAuth != null) {
            if (queueTiAuth.token() != null) {
                optBuilder.token(queueTiAuth.token());
            }
            optBuilder.tokenRefresher(queueTiAuth);
        } else if (props.getToken() != null) {
            optBuilder.token(props.getToken());
        }

        return QueueTiClient.connect(props.getGrpcAddress(), optBuilder.build());
    }

    // -------------------------------------------------------------------------
    // Producer
    // -------------------------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean(Producer.class)
    public Producer queueTiProducer(final QueueTiClient client) {
        return client.newProducer();
    }

    // -------------------------------------------------------------------------
    // AdminClient — optional, activated by queueti.admin.url
    // -------------------------------------------------------------------------

    /**
     * Creates an {@link AdminClient} bean.
     *
     * <p><b>Note:</b> {@code AdminClient} receives its token at creation time and will not
     * automatically pick up refreshed tokens. If your tokens are short-lived, inject
     * {@link QueueTiAuth} directly and re-create the admin client as needed.
     */
    @Bean
    @ConditionalOnProperty(prefix = "queueti.admin", name = "url")
    @ConditionalOnMissingBean(AdminClient.class)
    public AdminClient queueTiAdminClient(
            final QueueTiProperties props,
            @Autowired(required = false) final QueueTiAuth queueTiAuth) {

        final var adminProps = props.getAdmin();
        final var optBuilder = AdminOptions.builder()
                .requestTimeout(adminProps.getRequestTimeout());

        if (queueTiAuth != null && queueTiAuth.token() != null) {
            optBuilder.token(queueTiAuth.token());
        } else if (props.getToken() != null) {
            optBuilder.token(props.getToken());
        }

        return AdminClient.connect(adminProps.getUrl(), optBuilder.build());
    }
}
