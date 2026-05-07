package de.joesst.dev.queueti.spring.integration;

import java.util.concurrent.CompletableFuture;

/**
 * Settlement handle provided to downstream handlers in {@code MANUAL} acknowledge mode.
 *
 * <p>Call {@link #acknowledge()} to ack the message or {@link #nack(String)} to nack it.
 * Exactly one of the two must be called; calling both has no additional effect.
 *
 * <p>The adapter blocks its handler thread until settlement is signalled or the configured
 * timeout expires. Signalling is done via the injected {@link CompletableFuture} rather
 * than calling the queue-ti {@code msg.ack()}/{@code msg.nack()} directly, which avoids
 * double-acking: the consumer framework honours the handler's return/throw contract.
 */
public final class QueueTiAcknowledgment {

    private final CompletableFuture<Void> settlement;

    QueueTiAcknowledgment(final CompletableFuture<Void> settlement) {
        this.settlement = settlement;
    }

    public void acknowledge() {
        settlement.complete(null);
    }

    public void nack(final String reason) {
        settlement.completeExceptionally(new RuntimeException(reason));
    }
}
