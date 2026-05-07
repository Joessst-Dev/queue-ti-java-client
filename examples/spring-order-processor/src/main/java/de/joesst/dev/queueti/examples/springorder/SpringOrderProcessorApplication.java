package de.joesst.dev.queueti.examples.springorder;

import de.joesst.dev.queueti.Producer;
import de.joesst.dev.queueti.PublishOptions;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Spring Order Processor — demonstrates the Spring Boot starter and Spring Integration adapter
 * working together.
 *
 * <p>On startup, an {@link ApplicationRunner} publishes five sample orders to the {@code orders}
 * topic using the auto-configured {@link Producer} bean. An {@link OrderProcessingFlow}
 * {@code IntegrationFlow} simultaneously consumes those messages with MANUAL acknowledge mode,
 * routing validated orders to fulfillment and rejecting poison pills back to the server.
 *
 * <p>Requires queue-ti running locally: {@code docker compose up} from the queue-ti repo root.
 *
 * <p>Run: {@code ./gradlew :examples:spring-order-processor:bootRun}
 */
@SpringBootApplication
public class SpringOrderProcessorApplication {

    private static final Logger log =
            Logger.getLogger(SpringOrderProcessorApplication.class.getName());

    public static void main(final String[] args) {
        SpringApplication.run(SpringOrderProcessorApplication.class, args);
    }

    @Bean
    ApplicationRunner publishOrders(final Producer producer) {
        return args -> {
            var orders = List.of(
                    Map.of("id", "ord-1", "item", "Widget A",  "amount", "2",  "poison", "false"),
                    Map.of("id", "ord-2", "item", "Gadget B",  "amount", "1",  "poison", "false"),
                    Map.of("id", "ord-3", "item", "poison",    "amount", "0",  "poison", "true"),
                    Map.of("id", "ord-4", "item", "Widget C",  "amount", "5",  "poison", "false"),
                    Map.of("id", "ord-5", "item", "Gadget D",  "amount", "3",  "poison", "false")
            );

            log.info("Publishing " + orders.size() + " orders...");

            for (var order : orders) {
                var json = toJson(order);
                var opts = PublishOptions.builder()
                        .metadata(Map.of("source", "spring-order-processor"))
                        .key(order.get("id"))
                        .build();
                var messageId = producer.publish("orders", json.getBytes(StandardCharsets.UTF_8), opts).get();
                log.info("Published " + order.get("id") + " → " + messageId);
            }
        };
    }

    private static String toJson(final Map<String, String> fields) {
        var sb = new StringBuilder("{");
        var first = true;
        for (var entry : fields.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":");
            var val = entry.getValue();
            if (val.equals("true") || val.equals("false") || val.matches("\\d+")) {
                sb.append(val);
            } else {
                sb.append("\"").append(val).append("\"");
            }
            first = false;
        }
        return sb.append("}").toString();
    }
}
