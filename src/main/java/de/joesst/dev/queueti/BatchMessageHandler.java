package de.joesst.dev.queueti;

import java.util.List;

/**
 * Handles a batch of {@link Message messages} received from a queue-ti topic in a
 * single delivery.
 *
 * <p>Implementations are called by the consumer batch-dispatch loop. Returning
 * normally (even returning {@code null}) causes all messages in the batch to be
 * acknowledged. Throwing any exception causes all messages in the batch to be
 * negative-acknowledged with the exception message as the reason.
 *
 * <p>This is a functional interface; lambda expressions and method references may
 * be used wherever a {@code BatchMessageHandler} is expected.
 */
@FunctionalInterface
public interface BatchMessageHandler {

    /**
     * Processes a batch of messages.
     *
     * <p>Return {@code null} (or simply return normally) to acknowledge all messages.
     * Throw any exception to negative-acknowledge all messages in the batch.
     *
     * @param messages the list of messages to process; never {@code null}, never empty
     * @return {@code null} — the return type is {@code Void} to satisfy the functional interface
     * @throws Exception if processing fails; the exception message is forwarded as the nack reason
     */
    Void handle(List<Message> messages) throws Exception;
}
