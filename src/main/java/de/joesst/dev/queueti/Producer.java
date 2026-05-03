package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.QueueServiceGrpc;

/**
 * Publishes messages to a queue-ti topic.
 *
 * <p>TODO: Phase 8 — implement publish methods.
 */
public final class Producer {

    // TODO: Phase 8 — add publish(String topic, byte[] payload, PublishOptions options)
    //                  and publish(String topic, byte[] payload)

    private final QueueServiceGrpc.QueueServiceFutureStub futureStub;

    Producer(final QueueServiceGrpc.QueueServiceFutureStub futureStub) {
        this.futureStub = futureStub;
    }
}
