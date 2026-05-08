package de.joesst.dev.queueti.examples.springorder;

import de.joesst.dev.queueti.ConsumerOptions;
import de.joesst.dev.queueti.QueueTiClient;
import de.joesst.dev.queueti.spring.integration.QueueTiAcknowledgment;
import de.joesst.dev.queueti.spring.integration.QueueTiInboundChannelAdapter;
import de.joesst.dev.queueti.spring.integration.QueueTiMessageHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Spring Integration flow that consumes the {@code orders} topic.
 *
 * <p>Uses MANUAL acknowledge mode: the flow explicitly acks valid orders and nacks poison pills,
 * giving the server a meaningful rejection reason rather than a generic exception message.
 *
 * <p>Message flow:
 * <pre>
 *   QueueTiInboundChannelAdapter  →  DirectChannel  →  transform byte[]→String  →  handle
 * </pre>
 */
@Configuration
public class OrderProcessingFlow {

    private static final Logger log = Logger.getLogger(OrderProcessingFlow.class.getName());

    private static final String TOPIC          = "orders";
    private static final String CONSUMER_GROUP = "fulfillment";

    @Bean
    DirectChannel ordersChannel() {
        return new DirectChannel();
    }

    @Bean
    QueueTiInboundChannelAdapter orderAdapter(
            final QueueTiClient client,
            final DirectChannel ordersChannel) {
        // concurrency(3) spawns up to 3 virtual threads. DirectChannel dispatches each message
        // synchronously on its receiving thread, so all 3 can process in parallel without
        // an async executor channel.
        var adapter = new QueueTiInboundChannelAdapter(
                client,
                TOPIC,
                ConsumerOptions.builder().consumerGroup(CONSUMER_GROUP).concurrency(3).build());
        adapter.setAcknowledgeMode(QueueTiInboundChannelAdapter.AcknowledgeMode.MANUAL);
        adapter.setOutputChannel(ordersChannel);
        return adapter;
    }

    @Bean
    IntegrationFlow orderProcessingIntegrationFlow(
            final DirectChannel ordersChannel, final CountDownLatch orderLatch) {
        return IntegrationFlow.from(ordersChannel)
                .<byte[], String>transform(
                        payload -> new String(payload, StandardCharsets.UTF_8))
                .<String>handle((payload, headers) -> {
                    var ack = (QueueTiAcknowledgment)
                            headers.get(QueueTiMessageHeaders.ACKNOWLEDGMENT);
                    var id = headers.get(QueueTiMessageHeaders.MESSAGE_ID);

                    // Simplified check that relies on the exact format produced by toJson().
                    // A real application would deserialise the payload before inspecting fields.
                    if (payload.contains("\"poison\":true")) {
                        log.warning("Nacking poison pill " + id);
                        ack.nack("poison pill detected");
                    } else {
                        log.info("Fulfilling order " + id + ": " + payload);
                        ack.acknowledge();
                    }

                    orderLatch.countDown();
                    return null;
                })
                .get();
    }
}
