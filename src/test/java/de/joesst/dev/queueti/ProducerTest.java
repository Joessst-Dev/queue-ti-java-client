package de.joesst.dev.queueti;

import de.joesst.dev.queueti.pb.EnqueueRequest;
import de.joesst.dev.queueti.pb.EnqueueResponse;
import de.joesst.dev.queueti.pb.QueueServiceGrpc;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProducerTest {

    // -------------------------------------------------------------------------
    // Fake gRPC service
    // -------------------------------------------------------------------------

    /**
     * In-process implementation that captures every {@code EnqueueRequest} and responds with a
     * fixed ID. Set {@link #failWithInternalError} to {@code true} to simulate a server failure.
     */
    private static final class FakeEnqueueService extends QueueServiceGrpc.QueueServiceImplBase {

        final List<EnqueueRequest> capturedRequests = new ArrayList<>();
        boolean failWithInternalError = false;

        @Override
        public void enqueue(
                final EnqueueRequest request,
                final StreamObserver<EnqueueResponse> responseObserver) {
            if (failWithInternalError) {
                responseObserver.onError(
                        new StatusRuntimeException(Status.INTERNAL));
                return;
            }
            capturedRequests.add(request);
            responseObserver.onNext(
                    EnqueueResponse.newBuilder().setId("test-id-123").build());
            responseObserver.onCompleted();
        }
    }

    // -------------------------------------------------------------------------
    // Test infrastructure
    // -------------------------------------------------------------------------

    private FakeEnqueueService fakeService;
    private Server inProcessServer;
    private Producer producer;

    @BeforeEach
    void setUp() throws IOException {
        fakeService = new FakeEnqueueService();
        final var serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(fakeService)
                .build()
                .start();
        final var channel = InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build();
        final var stub = QueueServiceGrpc.newFutureStub(channel);
        producer = new Producer(stub);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        inProcessServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("publish returns the server-assigned message ID")
    void publish_returns_server_assigned_id() throws Exception {
        // Given
        final var payload = new byte[]{1, 2, 3};

        // When
        final var future = producer.publish("orders", payload);

        // Then
        assertThat(future.get()).isEqualTo("test-id-123");
    }

    @Test
    @DisplayName("publish with metadata options sends metadata entries to the server")
    void publish_with_options_sends_metadata() throws Exception {
        // Given
        final var options = PublishOptions.builder()
                .metadata(Map.of("k", "v"))
                .build();

        // When
        producer.publish("orders", new byte[]{1}, options).get();

        // Then
        final var captured = fakeService.capturedRequests.get(0);
        assertThat(captured.getMetadataMap()).containsEntry("k", "v");
    }

    @Test
    @DisplayName("publish with a routing key sends the key to the server")
    void publish_with_key_sends_key() throws Exception {
        // Given
        final var options = PublishOptions.builder()
                .key("order-42")
                .build();

        // When
        producer.publish("orders", new byte[]{1}, options).get();

        // Then
        final var captured = fakeService.capturedRequests.get(0);
        assertThat(captured.getKey()).isEqualTo("order-42");
    }

    @Test
    @DisplayName("publish without a routing key does not set the key field on the request")
    void publish_with_no_key_does_not_set_key() throws Exception {
        // Given
        final var options = PublishOptions.builder().build();

        // When
        producer.publish("orders", new byte[]{1}, options).get();

        // Then
        final var captured = fakeService.capturedRequests.get(0);
        assertThat(captured.hasKey()).isFalse();
    }

    @Test
    @DisplayName("publish throws IllegalArgumentException synchronously for a null topic")
    void publish_throws_for_null_topic() {
        // Given
        final byte[] payload = new byte[]{1};

        // When / Then
        assertThatThrownBy(() -> producer.publish(null, payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic must not be null or blank");
    }

    @Test
    @DisplayName("publish throws IllegalArgumentException synchronously for a blank topic")
    void publish_throws_for_blank_topic() {
        // Given
        final byte[] payload = new byte[]{1};

        // When / Then
        assertThatThrownBy(() -> producer.publish("", payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic must not be null or blank");
    }

    @Test
    @DisplayName("publish throws IllegalArgumentException synchronously for a null payload")
    void publish_throws_for_null_payload() {
        // When / Then
        assertThatThrownBy(() -> producer.publish("orders", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload must not be null");
    }

    @Test
    @DisplayName("publish completes the future exceptionally when the server returns an error")
    void publish_completes_exceptionally_on_server_error() {
        // Given
        fakeService.failWithInternalError = true;
        final var future = producer.publish("orders", new byte[]{1});

        // When / Then
        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(StatusRuntimeException.class);
    }
}
