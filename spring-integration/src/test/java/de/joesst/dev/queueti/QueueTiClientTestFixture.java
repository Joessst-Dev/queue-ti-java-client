package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import io.grpc.ManagedChannel;

/**
 * Test-only bridge exposing the package-private {@link QueueTiClient} constructor to
 * tests in other packages (e.g. spring-integration tests).
 */
public final class QueueTiClientTestFixture {

    private QueueTiClientTestFixture() {}

    public static QueueTiClient newClient(final ManagedChannel channel) {
        final TokenStore tokenStore = new TokenStore(null);
        return new QueueTiClient(
                channel,
                tokenStore,
                QueueServiceGrpc.newFutureStub(channel),
                QueueServiceGrpc.newStub(channel),
                null);
    }
}
