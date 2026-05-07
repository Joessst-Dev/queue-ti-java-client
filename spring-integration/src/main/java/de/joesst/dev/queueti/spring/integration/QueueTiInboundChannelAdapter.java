package de.joesst.dev.queueti.spring.integration;

import de.joesst.dev.queueti.Consumer;
import de.joesst.dev.queueti.ConsumerOptions;
import de.joesst.dev.queueti.Message;
import de.joesst.dev.queueti.QueueTiClient;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Spring Integration inbound channel adapter that consumes messages from a queue-ti topic
 * and publishes them as Spring Integration {@link org.springframework.messaging.Message}s.
 *
 * <p>Extends {@link MessageProducerSupport}, which implements {@link org.springframework.context.SmartLifecycle}.
 * Spring calls {@link #doStart()}/{@link #doStop()} automatically when the application
 * context starts and stops.
 *
 * <p>Two acknowledge modes are supported:
 * <ul>
 *   <li>{@link AcknowledgeMode#AUTO} — the adapter acks automatically when downstream
 *       processing returns normally; any exception causes a nack.</li>
 *   <li>{@link AcknowledgeMode#MANUAL} — downstream code must call
 *       {@link QueueTiAcknowledgment#acknowledge()} or {@link QueueTiAcknowledgment#nack(String)}
 *       on the header value keyed by {@link QueueTiMessageHeaders#ACKNOWLEDGMENT}.</li>
 * </ul>
 *
 * <pre>{@code
 * var adapter = new QueueTiInboundChannelAdapter(client, "orders");
 * adapter.setOutputChannel(channel);
 * adapter.setAcknowledgeMode(AcknowledgeMode.MANUAL);
 * adapter.afterPropertiesSet();
 * adapter.start();
 * }</pre>
 */
public final class QueueTiInboundChannelAdapter extends MessageProducerSupport {

    public enum AcknowledgeMode { AUTO, MANUAL }

    private final QueueTiClient client;
    private final String topic;
    private final ConsumerOptions consumerOptions;

    private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;
    private Duration settlementTimeout = Duration.ofSeconds(30);

    private volatile Thread consumerThread;

    public QueueTiInboundChannelAdapter(final QueueTiClient client, final String topic) {
        this(client, topic, ConsumerOptions.builder().build());
    }

    public QueueTiInboundChannelAdapter(
            final QueueTiClient client,
            final String topic,
            final ConsumerOptions consumerOptions) {
        this.client = client;
        this.topic = topic;
        this.consumerOptions = consumerOptions;
    }

    public void setAcknowledgeMode(final AcknowledgeMode acknowledgeMode) {
        this.acknowledgeMode = acknowledgeMode;
    }

    /** Timeout used in {@code MANUAL} mode waiting for downstream settlement. Default: 30s. */
    public void setSettlementTimeout(final Duration settlementTimeout) {
        this.settlementTimeout = settlementTimeout;
    }

    @Override
    public String getComponentType() {
        return "queueti:inbound-channel-adapter";
    }

    @Override
    protected void doStart() {
        final Consumer consumer = client.newConsumer(topic, consumerOptions);
        consumerThread = Thread.ofVirtual()
                .name("queueti-consumer-" + topic)
                .start(() -> consumer.consume(this::handle));
    }

    @Override
    protected void doStop() {
        final Thread t = consumerThread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Void handle(final Message msg) {
        if (acknowledgeMode == AcknowledgeMode.MANUAL) {
            return handleManual(msg);
        }
        return handleAuto(msg);
    }

    private Void handleAuto(final Message msg) {
        sendMessage(buildSpringMessage(msg, null));
        return null;
    }

    private Void handleManual(final Message msg) {
        final CompletableFuture<Void> settled = new CompletableFuture<>();
        final QueueTiAcknowledgment ack = new QueueTiAcknowledgment(settled);

        try {
            sendMessage(buildSpringMessage(msg, ack));
        } catch (final Exception e) {
            settled.completeExceptionally(e);
            throw e;
        }

        try {
            settled.get(settlementTimeout.toMillis(), MILLISECONDS);
            return null;
        } catch (final ExecutionException e) {
            throw new RuntimeException(e.getCause().getMessage());
        } catch (final TimeoutException e) {
            throw new RuntimeException("Settlement timeout after " + settlementTimeout);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for settlement");
        }
    }

    private org.springframework.messaging.Message<byte[]> buildSpringMessage(
            final Message msg,
            final QueueTiAcknowledgment acknowledgment) {
        final MessageBuilder<byte[]> builder = MessageBuilder
                .withPayload(msg.payload())
                .setHeader(QueueTiMessageHeaders.MESSAGE_ID, msg.id())
                .setHeader(QueueTiMessageHeaders.TOPIC, msg.topic())
                .setHeader(QueueTiMessageHeaders.RETRY_COUNT, msg.retryCount())
                .setHeader(QueueTiMessageHeaders.CREATED_AT, msg.createdAt())
                .setHeader(QueueTiMessageHeaders.METADATA, msg.metadata());

        if (acknowledgment != null) {
            builder.setHeader(QueueTiMessageHeaders.ACKNOWLEDGMENT, acknowledgment);
        }

        return builder.build();
    }
}
