package de.joesst.dev.queueti.examples.orderpipeline;

import de.joesst.dev.queueti.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Order pipeline — demonstrates the full producer → consumer → ack lifecycle.
 *
 * <p>Requires queue-ti running locally: {@code docker-compose up} from the repo root.
 *
 * <p>Run: {@code ./gradlew :examples:order-pipeline:run}
 */
public final class OrderPipeline {

    private static final Logger log = Logger.getLogger(OrderPipeline.class.getName());

    private static final String GRPC_ADDR      = "localhost:50051";
    private static final String ADMIN_ADDR     = "http://localhost:8080";
    private static final String TOPIC          = "orders";
    private static final String DLQ_TOPIC      = "orders.dlq";
    private static final String CONSUMER_GROUP = "fulfillment";

    // Default credentials shipped with the local docker-compose setup.
    private static final String DEFAULT_USERNAME = "admin";
    private static final String DEFAULT_PASSWORD = "secret";

    private record Order(String id, String item, int amount, boolean poison) {
        byte[] toJsonBytes() {
            var json = "{\"id\":\"" + id + "\","
                    + "\"item\":\"" + item + "\","
                    + "\"amount\":" + amount + ","
                    + "\"poison\":" + poison + "}";
            return json.getBytes(StandardCharsets.UTF_8);
        }
    }

    public static void main(final String[] args) throws Exception {
        final var mainThread = Thread.currentThread();
        // Interrupt the main thread on Ctrl-C so consume() unblocks and
        // the try-with-resources close() drains in-flight RPCs cleanly.
        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            mainThread.interrupt();
            try { mainThread.join(5_000); } catch (InterruptedException ignored) {}
        }));

        final var token = login();

        final var connectOptions = ConnectOptions.builder().insecure(true);
        if (token != null) {
            connectOptions.token(token);
        }

        try (var client = QueueTiClient.connect(GRPC_ADDR, connectOptions.build())) {

            registerConsumerGroup(token);

            // Produce in a background virtual thread so we can consume concurrently.
            Thread.ofVirtual().name("producer").start(() -> produce(client));

            // Drain the DLQ in a background virtual thread.
            Thread.ofVirtual().name("dlq-drainer").start(() -> drainDlq(client));

            // Blocking consume — exits when the main thread is interrupted (Ctrl-C).
            consume(client);
        }
    }

    // -------------------------------------------------------------------------
    // Auth
    // -------------------------------------------------------------------------

    /**
     * Checks whether the server requires authentication and, if so, logs in with
     * the default docker-compose credentials to obtain a JWT.
     *
     * @return the JWT, or {@code null} if auth is disabled or login fails
     */
    private static String login() throws IOException, InterruptedException {
        var http = HttpClient.newHttpClient();

        // Check if auth is required.
        var statusReq = HttpRequest.newBuilder()
                .uri(URI.create(ADMIN_ADDR + "/api/auth/status"))
                .GET()
                .build();
        var statusResp = http.send(statusReq, HttpResponse.BodyHandlers.ofString());
        if (!statusResp.body().contains("\"auth_required\":true")) {
            log.info("auth not required — connecting without token");
            return null;
        }

        // Log in with default credentials.
        var body = "{\"username\":\"" + DEFAULT_USERNAME + "\",\"password\":\"" + DEFAULT_PASSWORD + "\"}";
        var loginReq = HttpRequest.newBuilder()
                .uri(URI.create(ADMIN_ADDR + "/api/auth/login"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build();
        var loginResp = http.send(loginReq, HttpResponse.BodyHandlers.ofString());
        if (loginResp.statusCode() != 200) {
            throw new RuntimeException("login failed (HTTP " + loginResp.statusCode() + "): " + loginResp.body());
        }

        // Extract token from {"token":"eyJ..."}
        var raw = loginResp.body();
        var start = raw.indexOf("\"token\":\"") + 9;
        var end   = raw.indexOf("\"", start);
        var token = raw.substring(start, end);
        log.info("logged in as " + DEFAULT_USERNAME);
        return token;
    }

    // -------------------------------------------------------------------------
    // Consumer group registration
    // -------------------------------------------------------------------------

    private static void registerConsumerGroup(final String token) {
        var opts = token != null
                ? AdminOptions.builder().token(token).build()
                : AdminOptions.defaults();
        var admin = AdminClient.connect(ADMIN_ADDR, opts);
        try {
            admin.registerConsumerGroup(TOPIC, CONSUMER_GROUP);
            log.info("consumer group \"" + CONSUMER_GROUP + "\" registered");
        } catch (ConflictException e) {
            log.info("consumer group \"" + CONSUMER_GROUP + "\" already exists — continuing");
        }
    }

    // -------------------------------------------------------------------------
    // Producer
    // -------------------------------------------------------------------------

    private static void produce(final QueueTiClient client) {
        var producer = client.newProducer();
        var orders = List.of(
                new Order("ord-1", "Widget A",  2, false),
                new Order("ord-2", "Gadget B",  1, false),
                new Order("ord-3", "poison",    0, true),
                new Order("ord-4", "Widget C",  5, false),
                new Order("ord-5", "Gadget D",  3, false)
        );

        var futures = new ArrayList<CompletableFuture<?>>();
        for (var o : orders) {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            var opts = PublishOptions.builder()
                    .metadata(Map.of("source", "order-pipeline"))
                    .key(o.id())
                    .build();
            var future = producer.publish(TOPIC, o.toJsonBytes(), opts)
                    .thenAccept(id -> log.info("published " + o.id() + " → " + id))
                    .exceptionally(ex -> { log.warning("publish failed: " + ex.getMessage()); return null; });
            futures.add(future);
        }
        // Wait for all in-flight publishes to confirm before the thread exits.
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    // -------------------------------------------------------------------------
    // Streaming consumer
    // -------------------------------------------------------------------------

    private static void consume(final QueueTiClient client) {
        var consumer = client.newConsumer(TOPIC,
                ConsumerOptions.builder()
                        .consumerGroup(CONSUMER_GROUP)
                        .concurrency(3)
                        .build());

        log.info("consuming from \"" + TOPIC + "\" (group \"" + CONSUMER_GROUP + "\") — Ctrl-C to stop");

        consumer.consume(msg -> {
            var body = new String(msg.payload(), StandardCharsets.UTF_8);

            // Check the explicit poison flag in the JSON rather than searching the full body.
            if (body.contains("\"poison\":true")) {
                log.warning("nack " + msg.id() + ": poison pill detected (retry " + msg.retryCount() + ")");
                throw new RuntimeException("poison pill");
            }

            log.info("ack " + msg.id() + ": processed — " + body);
            return null;
        });
    }

    // -------------------------------------------------------------------------
    // DLQ drainer
    // -------------------------------------------------------------------------

    private static void drainDlq(final QueueTiClient client) {
        var consumer = client.newConsumer(DLQ_TOPIC,
                ConsumerOptions.builder().consumerGroup(CONSUMER_GROUP).build());

        log.info("draining DLQ \"" + DLQ_TOPIC + "\"");

        consumer.consumeBatch(10, messages -> {
            for (var msg : messages) {
                log.info("[DLQ] " + msg.id()
                        + " retry=" + msg.retryCount()
                        + " payload=" + new String(msg.payload(), StandardCharsets.UTF_8));
            }
            return null; // auto-ack all
        });
    }
}
