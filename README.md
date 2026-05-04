# queue-ti Java Client

A Java 21 gRPC client library for the [queue-ti](https://github.com/Joessst-Dev/queue-ti) message queue service.

## Requirements

- Java 21 LTS or later
- Gradle 8+ or Maven 3.9+

## Installation

### Gradle (Kotlin DSL)

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("de.joesst.dev:queue-ti-java-client:1.0-SNAPSHOT")
}
```

### Gradle (Groovy)

```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'de.joesst.dev:queue-ti-java-client:1.0-SNAPSHOT'
}
```

### Maven

```xml
<dependency>
    <groupId>de.joesst.dev</groupId>
    <artifactId>queue-ti-java-client</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## Quick Start

### Connect to the server

```java
try (var client = QueueTiClient.connect("localhost:50051",
        ConnectOptions.builder().insecure(true).build())) {
    // use client...
}
```

For TLS, omit the `insecure(true)` call or set it to `false`.

### Publish a message

```java
var producer = client.newProducer();
CompletableFuture<String> future = producer.publish("my-topic", "Hello".getBytes());
String messageId = future.get();
System.out.println("Published as: " + messageId);
```

With metadata and routing key:

```java
var options = PublishOptions.builder()
    .metadata(Map.of("source", "myapp", "version", "1.0"))
    .key("user-123")
    .build();
producer.publish("my-topic", payload, options).thenAccept(id ->
    System.out.println("Published: " + id)
);
```

### Consume messages (streaming)

```java
var consumer = client.newConsumer("my-topic",
    ConsumerOptions.builder().concurrency(5).consumerGroup("mygroup").build());

consumer.consume(message -> {
    System.out.println("Got: " + new String(message.payload()));
    // Return normally to ack, throw to nack
    return null;
});
```

The `consume()` call blocks until the thread is interrupted. Each message is dispatched on a virtual thread; concurrency is bounded by the configured level. Automatic exponential-backoff reconnection (500ms–30s) handles stream failures.

### Consume messages (batch polling)

```java
consumer.consumeBatch(10, messages -> {
    for (var msg : messages) {
        System.out.println("Processing: " + msg.id());
    }
    // All unacked messages are acked on return; throw to nack all
});
```

## Configuration

### ConnectOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `insecure` | `boolean` | `false` | Use plaintext channel (no TLS) |
| `token` | `String` | `null` | Static JWT to send on every request |
| `tokenRefresher` | `TokenRefresher` | `null` | Strategy to obtain fresh tokens dynamically |

### ConsumerOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `concurrency` | `int` | `1` | Max messages to dispatch concurrently |
| `consumerGroup` | `String` | `""` | Consumer group name (empty string is valid) |
| `visibilityTimeoutSeconds` | `Integer` | `null` | Message visibility timeout; `null` uses server default |

### PublishOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `metadata` | `Map<String, String>` | empty | Arbitrary key-value pairs attached to the message |
| `key` | `String` | `null` | Optional routing key for partitioning |

## Token Refresh

If you need dynamic token renewal (e.g., from OAuth2 or a token service), configure a `TokenRefresher`:

```java
TokenRefresher refresher = () -> myAuthClient.fetchToken();  // returns CompletableFuture<String>

var options = ConnectOptions.builder()
    .tokenRefresher(refresher)
    .build();

var client = QueueTiClient.connect("localhost:50051", options);
```

The client launches a background virtual thread that wakes 60 seconds before token expiry and calls your refresher. On failure, it retries with exponential backoff (5s–60s). The refresher has a 30-second timeout per call.

You can also update the token manually:

```java
client.setToken(newToken);
```

## Message

Every consumed message exposes:

| Field | Type | Description |
|-------|------|-------------|
| `id()` | `String` | Unique server-assigned message ID |
| `topic()` | `String` | Topic the message was published to |
| `payload()` | `byte[]` | Raw message bytes (defensive copy) |
| `metadata()` | `Map<String, String>` | Immutable metadata map |
| `createdAt()` | `Instant` | Wall-clock enqueue time; `Instant.EPOCH` if not set |
| `retryCount()` | `int` | Number of previous delivery attempts (0 on first try) |
| `maxRetries()` | `OptionalInt` | Server-configured max retries (batch-only; empty for streaming) |

Acknowledge or negative-acknowledge:

```java
message.ack()                           // success
message.nack("processing error")        // failure with reason
```

Both return a `CompletableFuture<Void>` that completes when the server confirms.

## Building from Source

```bash
./gradlew build                         # compile + all tests
./gradlew test                          # tests only
./gradlew clean                         # remove build outputs
./gradlew generateProto                 # regenerate gRPC stubs from proto
```

Tests use JUnit 5 with an in-process gRPC server — no external server or mocks needed.

## Related

- **queue-ti server**: https://github.com/Joessst-Dev/queue-ti
- **Reference clients**: Go, Node.js, Python clients in the same repo demonstrate async patterns and edge-case handling.
