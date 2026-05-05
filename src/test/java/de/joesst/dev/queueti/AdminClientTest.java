package de.joesst.dev.queueti;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdminClientTest {

    // -------------------------------------------------------------------------
    // In-process HTTP server infrastructure
    // -------------------------------------------------------------------------

    /**
     * Captures the most recent incoming request (method, path, body, Authorization header) and
     * responds with whatever body/status code has been configured.
     */
    private static final class FakeHttpServer {

        private final HttpServer server;
        final AtomicReference<String> lastMethod = new AtomicReference<>();
        final AtomicReference<String> lastPath = new AtomicReference<>();
        final AtomicReference<String> lastBody = new AtomicReference<>();
        final AtomicReference<String> lastAuthHeader = new AtomicReference<>();

        private volatile int responseStatus = 200;
        private volatile String responseBody = "{}";

        FakeHttpServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", exchange -> {
                lastMethod.set(exchange.getRequestMethod());
                lastPath.set(exchange.getRequestURI().getPath());
                lastBody.set(new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8));
                lastAuthHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));

                final var bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(responseStatus, bytes.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(bytes);
                }
            });
            server.start();
        }

        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        void willRespondWith(final int status, final String body) {
            responseStatus = status;
            responseBody = body;
        }

        void stop() {
            server.stop(0);
        }
    }

    // -------------------------------------------------------------------------
    // Test fixtures
    // -------------------------------------------------------------------------

    private FakeHttpServer fakeServer;
    private AdminClient adminClient;

    @BeforeEach
    void setUp() throws IOException {
        fakeServer = new FakeHttpServer();
        adminClient = AdminClient.connect(fakeServer.baseUrl(), AdminOptions.defaults());
    }

    @AfterEach
    void tearDown() {
        fakeServer.stop();
    }

    // -------------------------------------------------------------------------
    // listTopicConfigs
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("listTopicConfigs returns parsed list from items array")
    void listTopicConfigs_returns_parsed_list() {
        // Given
        fakeServer.willRespondWith(200, """
                {"items":[{"topic":"orders","replayable":true,"max_retries":3}]}
                """);

        // When
        final var configs = adminClient.listTopicConfigs();

        // Then
        assertThat(configs).hasSize(1);
        assertThat(configs.get(0).topic()).isEqualTo("orders");
        assertThat(configs.get(0).replayable()).isTrue();
        assertThat(configs.get(0).maxRetries()).isEqualTo(3);
    }

    @Test
    @DisplayName("listTopicConfigs returns empty list when items array is empty")
    void listTopicConfigs_returns_empty_list_when_no_items() {
        // Given
        fakeServer.willRespondWith(200, "{\"items\":[]}");

        // When
        final var configs = adminClient.listTopicConfigs();

        // Then
        assertThat(configs).isNotNull().isEmpty();
    }

    // -------------------------------------------------------------------------
    // upsertTopicConfig
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("upsertTopicConfig sends PUT to correct path and returns parsed response")
    void upsertTopicConfig_sends_put_and_returns_saved_config() {
        // Given
        fakeServer.willRespondWith(200,
                "{\"topic\":\"orders\",\"replayable\":false}");
        final var config = new TopicConfig("orders", false, null, null, null, null, null);

        // When
        final var result = adminClient.upsertTopicConfig("orders", config);

        // Then
        assertThat(fakeServer.lastMethod.get()).isEqualTo("PUT");
        assertThat(fakeServer.lastPath.get()).isEqualTo("/api/topic-configs/orders");
        assertThat(result.topic()).isEqualTo("orders");
        assertThat(result.replayable()).isFalse();
    }

    @Test
    @DisplayName("upsertTopicConfig omits null optional fields from request body")
    void upsertTopicConfig_omits_null_fields() {
        // Given
        fakeServer.willRespondWith(200, "{\"topic\":\"t\",\"replayable\":false}");
        final var config = new TopicConfig("t", false, null, null, null, null, null);

        // When
        adminClient.upsertTopicConfig("t", config);

        // Then
        final var body = fakeServer.lastBody.get();
        assertThat(body).doesNotContain("max_retries");
        assertThat(body).doesNotContain("message_ttl_seconds");
        assertThat(body).doesNotContain("max_depth");
        assertThat(body).doesNotContain("replay_window_seconds");
        assertThat(body).doesNotContain("throughput_limit");
        assertThat(body).contains("\"topic\":\"t\"");
        assertThat(body).contains("\"replayable\":false");
    }

    @Test
    @DisplayName("upsertTopicConfig includes non-null optional fields in request body")
    void upsertTopicConfig_includes_non_null_fields() {
        // Given
        fakeServer.willRespondWith(200, "{\"topic\":\"t\",\"replayable\":true,\"max_retries\":5}");
        final var config = new TopicConfig("t", true, 5, 3600, 1000, 86400, 100);

        // When
        adminClient.upsertTopicConfig("t", config);

        // Then
        final var body = fakeServer.lastBody.get();
        assertThat(body).contains("\"max_retries\":5");
        assertThat(body).contains("\"message_ttl_seconds\":3600");
        assertThat(body).contains("\"max_depth\":1000");
        assertThat(body).contains("\"replay_window_seconds\":86400");
        assertThat(body).contains("\"throughput_limit\":100");
    }

    // -------------------------------------------------------------------------
    // deleteTopicConfig
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("deleteTopicConfig sends DELETE to correct path and returns normally on 204")
    void deleteTopicConfig_sends_delete_on_204() {
        // Given
        fakeServer.willRespondWith(204, "");

        // When / Then (no exception)
        adminClient.deleteTopicConfig("orders");

        assertThat(fakeServer.lastMethod.get()).isEqualTo("DELETE");
        assertThat(fakeServer.lastPath.get()).isEqualTo("/api/topic-configs/orders");
    }

    // -------------------------------------------------------------------------
    // listTopicSchemas
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("listTopicSchemas returns parsed list from items array")
    void listTopicSchemas_returns_parsed_list() {
        // Given
        fakeServer.willRespondWith(200, """
                {"items":[{"topic":"orders","schema_json":"{}","version":1,"updated_at":"2024-01-01T00:00:00Z"}]}
                """);

        // When
        final var schemas = adminClient.listTopicSchemas();

        // Then
        assertThat(schemas).hasSize(1);
        assertThat(schemas.get(0).topic()).isEqualTo("orders");
        assertThat(schemas.get(0).schemaJson()).isEqualTo("{}");
        assertThat(schemas.get(0).version()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // getTopicSchema
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("getTopicSchema returns parsed schema for existing topic")
    void getTopicSchema_returns_parsed_schema() {
        // Given
        fakeServer.willRespondWith(200,
                "{\"topic\":\"orders\",\"schema_json\":\"{}\",\"version\":2,\"updated_at\":\"2024-01-01T00:00:00Z\"}");

        // When
        final var schema = adminClient.getTopicSchema("orders");

        // Then
        assertThat(schema.topic()).isEqualTo("orders");
        assertThat(schema.version()).isEqualTo(2);
    }

    @Test
    @DisplayName("getTopicSchema throws NotFoundException when server returns 404")
    void getTopicSchema_throws_not_found_on_404() {
        // Given
        fakeServer.willRespondWith(404, "{\"error\":\"not found\"}");

        // When / Then
        assertThatThrownBy(() -> adminClient.getTopicSchema("unknown"))
                .isInstanceOf(NotFoundException.class);
    }

    // -------------------------------------------------------------------------
    // upsertTopicSchema
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("upsertTopicSchema sends PUT with schema_json body and returns parsed response")
    void upsertTopicSchema_sends_put_with_schema_json() {
        // Given
        fakeServer.willRespondWith(200,
                "{\"topic\":\"orders\",\"schema_json\":\"{}\",\"version\":1,\"updated_at\":\"2024-01-01T00:00:00Z\"}");

        // When
        final var schema = adminClient.upsertTopicSchema("orders", "{}");

        // Then
        assertThat(fakeServer.lastMethod.get()).isEqualTo("PUT");
        assertThat(fakeServer.lastPath.get()).isEqualTo("/api/topic-schemas/orders");
        assertThat(fakeServer.lastBody.get()).contains("\"schema_json\":\"{}\"");
        assertThat(schema.topic()).isEqualTo("orders");
    }

    // -------------------------------------------------------------------------
    // deleteTopicSchema
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("deleteTopicSchema sends DELETE to correct path and returns normally on 204")
    void deleteTopicSchema_sends_delete_on_204() {
        // Given
        fakeServer.willRespondWith(204, "");

        // When
        adminClient.deleteTopicSchema("orders");

        // Then
        assertThat(fakeServer.lastMethod.get()).isEqualTo("DELETE");
        assertThat(fakeServer.lastPath.get()).isEqualTo("/api/topic-schemas/orders");
    }

    // -------------------------------------------------------------------------
    // listConsumerGroups
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("listConsumerGroups returns list of group name strings")
    void listConsumerGroups_returns_string_list() {
        // Given
        fakeServer.willRespondWith(200, "{\"items\":[\"group-a\",\"group-b\"]}");

        // When
        final var groups = adminClient.listConsumerGroups("orders");

        // Then
        assertThat(groups).containsExactly("group-a", "group-b");
        assertThat(fakeServer.lastPath.get()).isEqualTo("/api/topics/orders/consumer-groups");
    }

    // -------------------------------------------------------------------------
    // registerConsumerGroup
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("registerConsumerGroup sends POST with consumer_group body")
    void registerConsumerGroup_sends_post_with_body() {
        // Given
        fakeServer.willRespondWith(201, "{}");

        // When
        adminClient.registerConsumerGroup("orders", "billing");

        // Then
        assertThat(fakeServer.lastMethod.get()).isEqualTo("POST");
        assertThat(fakeServer.lastPath.get()).isEqualTo("/api/topics/orders/consumer-groups");
        assertThat(fakeServer.lastBody.get()).contains("\"consumer_group\":\"billing\"");
    }

    @Test
    @DisplayName("registerConsumerGroup throws ConflictException when server returns 409")
    void registerConsumerGroup_throws_conflict_on_409() {
        // Given
        fakeServer.willRespondWith(409, "{\"error\":\"already exists\"}");

        // When / Then
        assertThatThrownBy(() -> adminClient.registerConsumerGroup("orders", "billing"))
                .isInstanceOf(ConflictException.class);
    }

    // -------------------------------------------------------------------------
    // unregisterConsumerGroup
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("unregisterConsumerGroup sends DELETE to correct path")
    void unregisterConsumerGroup_sends_delete() {
        // Given
        fakeServer.willRespondWith(204, "");

        // When
        adminClient.unregisterConsumerGroup("orders", "billing");

        // Then
        assertThat(fakeServer.lastMethod.get()).isEqualTo("DELETE");
        assertThat(fakeServer.lastPath.get())
                .isEqualTo("/api/topics/orders/consumer-groups/billing");
    }

    @Test
    @DisplayName("unregisterConsumerGroup throws NotFoundException when server returns 404")
    void unregisterConsumerGroup_throws_not_found_on_404() {
        // Given
        fakeServer.willRespondWith(404, "{\"error\":\"not found\"}");

        // When / Then
        assertThatThrownBy(() -> adminClient.unregisterConsumerGroup("orders", "missing"))
                .isInstanceOf(NotFoundException.class);
    }

    // -------------------------------------------------------------------------
    // stats
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("stats returns parsed list from topics array")
    void stats_returns_parsed_list() {
        // Given
        fakeServer.willRespondWith(200,
                "{\"topics\":[{\"topic\":\"orders\",\"status\":\"active\",\"count\":42}]}");

        // When
        final var stats = adminClient.stats();

        // Then
        assertThat(stats).hasSize(1);
        assertThat(stats.get(0).topic()).isEqualTo("orders");
        assertThat(stats.get(0).status()).isEqualTo("active");
        assertThat(stats.get(0).count()).isEqualTo(42);
    }

    // -------------------------------------------------------------------------
    // Authorization header
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Authorization header is sent when a token is configured")
    void authorization_header_is_present_when_token_set() {
        // Given
        fakeServer.willRespondWith(200, "{\"items\":[]}");
        final var client = AdminClient.connect(fakeServer.baseUrl(),
                AdminOptions.builder().token("my-jwt").build());

        // When
        client.listTopicConfigs();

        // Then
        assertThat(fakeServer.lastAuthHeader.get()).isEqualTo("Bearer my-jwt");
    }

    @Test
    @DisplayName("Authorization header is absent when no token is configured")
    void authorization_header_is_absent_when_no_token() {
        // Given
        fakeServer.willRespondWith(200, "{\"items\":[]}");
        final var client = AdminClient.connect(fakeServer.baseUrl(), AdminOptions.defaults());

        // When
        client.listTopicConfigs();

        // Then
        assertThat(fakeServer.lastAuthHeader.get()).isNull();
    }
}
