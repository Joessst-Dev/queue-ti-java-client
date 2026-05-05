package de.joesst.dev.queueti;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

/**
 * Synchronous client for the queue-ti HTTP admin REST API.
 *
 * <p>Obtain an instance via {@link #connect(String, AdminOptions)}:
 *
 * <pre>{@code
 * var admin = AdminClient.connect("http://localhost:8080",
 *         AdminOptions.builder().token("eyJ...").build());
 * List<TopicConfig> configs = admin.listTopicConfigs();
 * }</pre>
 *
 * <p>All methods are synchronous (blocking). Thread-safe: the underlying {@link HttpClient} is
 * stateless per-request.
 */
public final class AdminClient {

    private static final com.google.gson.Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private final HttpClient httpClient;
    private final String baseUrl;
    private final AdminOptions options;

    private AdminClient(final HttpClient httpClient, final String baseUrl,
            final AdminOptions options) {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
        this.options = options;
    }

    /**
     * Creates an {@code AdminClient} connected to the given base URL.
     *
     * @param baseUrl the root URL of the queue-ti admin API (e.g. {@code http://localhost:8080});
     *                must not be {@code null}
     * @param options client configuration; must not be {@code null}
     * @return a ready-to-use {@code AdminClient}
     * @throws IllegalArgumentException if {@code baseUrl} or {@code options} is {@code null}
     */
    public static AdminClient connect(final String baseUrl, final AdminOptions options) {
        if (baseUrl == null) {
            throw new IllegalArgumentException("baseUrl must not be null");
        }
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        final var httpClient = HttpClient.newBuilder()
                .connectTimeout(options.getRequestTimeout())
                .build();
        final var normalised = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        return new AdminClient(httpClient, normalised, options);
    }

    // -------------------------------------------------------------------------
    // Topic configs
    // -------------------------------------------------------------------------

    /**
     * Lists all topic configurations.
     *
     * @return an unmodifiable list of topic configurations; never {@code null}
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public List<TopicConfig> listTopicConfigs() {
        final var response = get("/api/topic-configs");
        final var root = GSON.fromJson(response, JsonObject.class);
        return GSON.fromJson(root.get("items"), new TypeToken<List<TopicConfig>>() {}.getType());
    }

    /**
     * Creates or updates the configuration for a topic.
     *
     * @param topic  the topic name; must not be {@code null}
     * @param config the configuration to persist; must not be {@code null}
     * @return the saved configuration as returned by the server
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public TopicConfig upsertTopicConfig(final String topic, final TopicConfig config) {
        final var body = GSON.toJson(topicConfigToJson(config));
        final var response = put("/api/topic-configs/" + encode(topic), body);
        return GSON.fromJson(response, TopicConfig.class);
    }

    /**
     * Deletes the configuration for a topic.
     *
     * @param topic the topic name; must not be {@code null}
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public void deleteTopicConfig(final String topic) {
        delete("/api/topic-configs/" + encode(topic));
    }

    // -------------------------------------------------------------------------
    // Topic schemas
    // -------------------------------------------------------------------------

    /**
     * Lists all topic schemas.
     *
     * @return an unmodifiable list of topic schemas; never {@code null}
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public List<TopicSchema> listTopicSchemas() {
        final var response = get("/api/topic-schemas");
        final var root = GSON.fromJson(response, JsonObject.class);
        return GSON.fromJson(root.get("items"), new TypeToken<List<TopicSchema>>() {}.getType());
    }

    /**
     * Returns the schema for a single topic.
     *
     * @param topic the topic name; must not be {@code null}
     * @return the topic schema
     * @throws NotFoundException    if the topic schema does not exist (HTTP 404)
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public TopicSchema getTopicSchema(final String topic) {
        final var response = get("/api/topic-schemas/" + encode(topic));
        return GSON.fromJson(response, TopicSchema.class);
    }

    /**
     * Creates or updates the schema for a topic.
     *
     * @param topic      the topic name; must not be {@code null}
     * @param schemaJson the JSON Schema document; must not be {@code null}
     * @return the saved schema as returned by the server
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public TopicSchema upsertTopicSchema(final String topic, final String schemaJson) {
        final var bodyObj = new JsonObject();
        bodyObj.addProperty("schema_json", schemaJson);
        final var response = put("/api/topic-schemas/" + encode(topic), GSON.toJson(bodyObj));
        return GSON.fromJson(response, TopicSchema.class);
    }

    /**
     * Deletes the schema for a topic.
     *
     * @param topic the topic name; must not be {@code null}
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public void deleteTopicSchema(final String topic) {
        delete("/api/topic-schemas/" + encode(topic));
    }

    // -------------------------------------------------------------------------
    // Consumer groups
    // -------------------------------------------------------------------------

    /**
     * Lists all consumer groups registered for a topic.
     *
     * @param topic the topic name; must not be {@code null}
     * @return a list of consumer group names; never {@code null}
     * @throws NotFoundException    if the topic does not exist (HTTP 404)
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public List<String> listConsumerGroups(final String topic) {
        final var response = get("/api/topics/" + encode(topic) + "/consumer-groups");
        final var root = GSON.fromJson(response, JsonObject.class);
        return GSON.fromJson(root.get("items"), new TypeToken<List<String>>() {}.getType());
    }

    /**
     * Registers a consumer group for a topic.
     *
     * @param topic the topic name; must not be {@code null}
     * @param group the consumer group name; must not be {@code null}
     * @throws ConflictException    if the consumer group already exists (HTTP 409)
     * @throws NotFoundException    if the server returns 404
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public void registerConsumerGroup(final String topic, final String group) {
        final var bodyObj = new JsonObject();
        bodyObj.addProperty("consumer_group", group);
        post("/api/topics/" + encode(topic) + "/consumer-groups", GSON.toJson(bodyObj));
    }

    /**
     * Unregisters a consumer group from a topic.
     *
     * @param topic the topic name; must not be {@code null}
     * @param group the consumer group name; must not be {@code null}
     * @throws NotFoundException    if the consumer group does not exist (HTTP 404)
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public void unregisterConsumerGroup(final String topic, final String group) {
        delete("/api/topics/" + encode(topic) + "/consumer-groups/" + encode(group));
    }

    // -------------------------------------------------------------------------
    // Stats
    // -------------------------------------------------------------------------

    /**
     * Returns runtime statistics for all topics.
     *
     * @return a list of per-topic statistics; never {@code null}
     * @throws NotFoundException    if the server returns 404
     * @throws ConflictException    if the server returns 409
     * @throws UncheckedIOException if the request fails or the server returns any other error
     */
    public List<TopicStat> stats() {
        final var response = get("/api/stats");
        final var root = GSON.fromJson(response, JsonObject.class);
        return GSON.fromJson(root.get("topics"), new TypeToken<List<TopicStat>>() {}.getType());
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private String get(final String path) {
        final var request = requestBuilder(path).GET().build();
        return execute(request);
    }

    private String put(final String path, final String jsonBody) {
        final var request = requestBuilder(path)
                .PUT(HttpRequest.BodyPublishers.ofString(jsonBody))
                .header("Content-Type", "application/json")
                .build();
        return execute(request);
    }

    private String post(final String path, final String jsonBody) {
        final var request = requestBuilder(path)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .header("Content-Type", "application/json")
                .build();
        return execute(request);
    }

    private void delete(final String path) {
        final var request = requestBuilder(path).DELETE().build();
        execute(request);
    }

    private HttpRequest.Builder requestBuilder(final String path) {
        final var builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(options.getRequestTimeout());
        if (options.getToken() != null) {
            builder.header("Authorization", "Bearer " + options.getToken());
        }
        return builder;
    }

    private String execute(final HttpRequest request) {
        final HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException("HTTP request failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException("HTTP request interrupted",
                    new IOException(e.getMessage(), e));
        }

        final int status = response.statusCode();
        if (status == 404) {
            throw new NotFoundException("Not found: " + request.uri());
        }
        if (status == 409) {
            throw new ConflictException("Conflict: " + request.uri());
        }
        if (status < 200 || status >= 300) {
            throw new UncheckedIOException(
                    "HTTP " + status + " from " + request.uri(),
                    new IOException("unexpected HTTP status " + status));
        }
        return response.body();
    }

    // -------------------------------------------------------------------------
    // Serialisation helpers
    // -------------------------------------------------------------------------

    private static JsonObject topicConfigToJson(final TopicConfig cfg) {
        final var obj = new JsonObject();
        obj.addProperty("topic", cfg.topic());
        obj.addProperty("replayable", cfg.replayable());
        if (cfg.maxRetries() != null)         obj.addProperty("max_retries", cfg.maxRetries());
        if (cfg.messageTtlSeconds() != null)  obj.addProperty("message_ttl_seconds", cfg.messageTtlSeconds());
        if (cfg.maxDepth() != null)           obj.addProperty("max_depth", cfg.maxDepth());
        if (cfg.replayWindowSeconds() != null) obj.addProperty("replay_window_seconds", cfg.replayWindowSeconds());
        if (cfg.throughputLimit() != null)    obj.addProperty("throughput_limit", cfg.throughputLimit());
        return obj;
    }

    private static String encode(final String segment) {
        return segment.replace(" ", "%20");
    }
}
