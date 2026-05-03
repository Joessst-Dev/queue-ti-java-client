package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.QueueServiceGrpc;

/**
 * Consumes messages from a queue-ti topic.
 *
 * <p>TODO: Phase 9 — implement subscribe / ack / nack methods.
 */
public final class Consumer {

    // TODO: Phase 9 — add subscribe(MessageHandler handler), ack(String messageId),
    //                  nack(String messageId)

    private final QueueServiceGrpc.QueueServiceStub asyncStub;
    private final QueueServiceGrpc.QueueServiceFutureStub futureStub;
    private final String topic;
    private final ConsumerOptions options;

    Consumer(
            final QueueServiceGrpc.QueueServiceStub asyncStub,
            final QueueServiceGrpc.QueueServiceFutureStub futureStub,
            final String topic,
            final ConsumerOptions options) {
        this.asyncStub = asyncStub;
        this.futureStub = futureStub;
        this.topic = topic;
        this.options = options;
    }
}
