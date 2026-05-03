package de.joesst.dev.queueti;

/**
 * Handles a single {@link Message} received from a queue-ti topic.
 *
 * <p>Implementations are called by the consumer dispatch loop. Returning
 * normally (even returning {@code null}) causes the message to be acknowledged.
 * Throwing any exception causes the message to be negative-acknowledged with the
 * exception message as the reason.
 *
 * <p>This is a functional interface; lambda expressions and method references may
 * be used wherever a {@code MessageHandler} is expected.
 */
@FunctionalInterface
public interface MessageHandler {

    /**
     * Processes a single message.
     *
     * <p>Return {@code null} (or simply return normally) to acknowledge the message.
     * Throw any exception to negative-acknowledge it.
     *
     * @param message the message to process
     * @return {@code null} — the return type is {@code Void} to satisfy the functional interface
     * @throws Exception if processing fails; the exception message is forwarded as the nack reason
     */
    Void handle(Message message) throws Exception;
}
