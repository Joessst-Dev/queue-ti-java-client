package de.joesst.dev.queueti.spring;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueueTiPropertiesBindingTest {

    private QueueTiProperties bind(final Map<String, String> props) {
        final var source = new MapConfigurationPropertySource(props);
        return new Binder(source).bindOrCreate("queueti", QueueTiProperties.class);
    }

    @Test
    @DisplayName("defaults: insecure=false, admin timeout=30s, all others null")
    void defaults() {
        final var props = bind(Map.of("queueti.grpc-address", "localhost:50051"));

        assertThat(props.getGrpcAddress()).isEqualTo("localhost:50051");
        assertThat(props.isInsecure()).isFalse();
        assertThat(props.getToken()).isNull();
        assertThat(props.getTls().hasAnyValue()).isFalse();
        assertThat(props.getAuth().getAdminAddress()).isNull();
        assertThat(props.getAdmin().getUrl()).isNull();
        assertThat(props.getAdmin().getRequestTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("insecure=true binds correctly")
    void insecure() {
        final var props = bind(Map.of(
                "queueti.grpc-address", "myserver:50051",
                "queueti.insecure", "true"));

        assertThat(props.isInsecure()).isTrue();
    }

    @Test
    @DisplayName("auth properties bind correctly")
    void authProperties() {
        final var props = bind(Map.of(
                "queueti.grpc-address", "localhost:50051",
                "queueti.auth.admin-address", "http://localhost:8080",
                "queueti.auth.username", "admin",
                "queueti.auth.password", "secret"));

        assertThat(props.getAuth().getAdminAddress()).isEqualTo("http://localhost:8080");
        assertThat(props.getAuth().getUsername()).isEqualTo("admin");
        assertThat(props.getAuth().getPassword()).isEqualTo("secret");
    }

    @Test
    @DisplayName("admin properties bind correctly, including custom timeout")
    void adminProperties() {
        final var props = bind(Map.of(
                "queueti.grpc-address", "localhost:50051",
                "queueti.admin.url", "http://localhost:8080",
                "queueti.admin.request-timeout", "10s"));

        assertThat(props.getAdmin().getUrl()).isEqualTo("http://localhost:8080");
        assertThat(props.getAdmin().getRequestTimeout()).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("tls server-name-override binds and hasAnyValue() returns true")
    void tlsServerNameOverride() {
        final var props = bind(Map.of(
                "queueti.grpc-address", "localhost:50051",
                "queueti.tls.server-name-override", "myserver.internal"));

        assertThat(props.getTls().getServerNameOverride()).isEqualTo("myserver.internal");
        assertThat(props.getTls().hasAnyValue()).isTrue();
    }

    @Test
    @DisplayName("static token binds correctly")
    void staticToken() {
        final var props = bind(Map.of(
                "queueti.grpc-address", "localhost:50051",
                "queueti.token", "eyJhbGciOiJIUzI1NiJ9.test.sig"));

        assertThat(props.getToken()).isEqualTo("eyJhbGciOiJIUzI1NiJ9.test.sig");
    }
}
