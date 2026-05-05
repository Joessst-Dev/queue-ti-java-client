package de.joesst.dev.queueti.examples.orderpipeline;

import de.joesst.dev.queueti.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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

    private static final String GRPC_ADDR     = "localhost:50051";
    private static final String ADMIN_ADDR    = "http://localhost:8080";
    private static final String TOPIC         = "orders";
    private static final String DLQ_TOPIC     = "orders.dlq";
    private static final String CONSUMER_GROUP = "fulfillment";

    // Payload for the poison-pill order that will be nacked and dead-lettered.
    private static final String POISON_MARKER = "POISON";

    public static void main(final String[] args) throws Exception {
        try (var client = QueueTiClient.connect(GRPC_ADDR,
                ConnectOptions.builder().insecure(true).build())) {

            registerConsumerGroup();

            // Produce in a background virtual thread so we can consume concurrently.
            Thread.ofVirtual().name("producer").start(() -> produce(client));

            // Drain the DLQ in a background virtual thread.
            Thread.ofVirtual().name("dlq-drainer").start(() -> drainDlq(client));

            // Blocking consume — exits on interrupt (Ctrl-C).
            consume(client);
        }
    }

    // -------------------------------------------------------------------------
    // Consumer group registration
    // -------------------------------------------------------------------------

    private static void registerConsumerGroup() {
        var admin = AdminClient.connect(ADMIN_ADDR, AdminOptions.defaults());
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
                order("ord-1", "Widget A",  2, false),
                order("ord-2", "Gadget B",  1, false),
                order("ord-3", POISON_MARKER, 0, true),
                order("ord-4", "Widget C",  5, false),
                order("ord-5", "Gadget D",  3, false)
        );

        for (var o : orders) {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            var opts = PublishOptions.builder()
                    .metadata(Map.of("source", "order-pipeline"))
                    .key((String) o[0])
                    .build();
            producer.publish(TOPIC, payload(o), opts)
                    .thenAccept(id -> log.info("published " + o[0] + " → " + id))
                    .exceptionally(ex -> { log.warning("publish failed: " + ex.getMessage()); return null; });
        }
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

            if (body.contains(POISON_MARKER)) {
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Returns [id, item, amount, poison] as a simple Object array. */
    private static Object[] order(final String id, final String item,
                                   final int amount, final boolean poison) {
        return new Object[]{id, item, amount, poison};
    }

    private static byte[] payload(final Object[] o) {
        var json = "{\"id\":\"" + o[0] + "\","
                + "\"item\":\"" + o[1] + "\","
                + "\"amount\":" + o[2] + ","
                + "\"poison\":" + o[3] + "}";
        return json.getBytes(StandardCharsets.UTF_8);
    }
}
