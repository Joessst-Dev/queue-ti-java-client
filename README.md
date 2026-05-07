# queue-ti Java Client

A Java 21 gRPC client library for the [queue-ti](https://github.com/Joessst-Dev/queue-ti) message queue service — a lightweight gRPC-based queue with at-least-once delivery, consumer groups, and visibility timeouts.

## Requirements

- Java 21 LTS or later
- Gradle 8+ or Maven 3.9+

## Installation

Releases are published to **GitHub Packages**. GitHub Packages requires authentication even for public repositories, so you need a Personal Access Token (PAT) before adding the dependency.

### 1. Create a Personal Access Token

Go to **GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)** and generate a token with the `read:packages` scope. Store it somewhere safe.

### 2. Add credentials to your Gradle or Maven configuration

**Gradle** — add to `~/.gradle/gradle.properties` (never commit this file):

```properties
gpr.user=your-github-username
gpr.key=ghp_xxxxxxxxxxxxxxxxxxxx
```

**Maven** — add to `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>github-queue-ti</id>
      <username>your-github-username</username>
      <password>ghp_xxxxxxxxxxxxxxxxxxxx</password>
    </server>
  </servers>
</settings>
```

### 3. Declare the repository and dependency

Replace `VERSION` with the desired release (e.g. `2026.05.0`). See [releases](https://github.com/Joessst-Dev/queue-ti-java-client/releases) for available versions.

**Gradle (Kotlin DSL)**

```kotlin
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/Joessst-Dev/queue-ti-java-client")
        credentials {
            username = providers.gradleProperty("gpr.user").orNull
            password = providers.gradleProperty("gpr.key").orNull
        }
    }
}

dependencies {
    implementation("de.joesst.dev:queue-ti-java-client:VERSION")
}
```

**Gradle (Groovy)**

```groovy
repositories {
    maven {
        url 'https://maven.pkg.github.com/Joessst-Dev/queue-ti-java-client'
        credentials {
            username = findProperty('gpr.user')
            password = findProperty('gpr.key')
        }
    }
}

dependencies {
    implementation 'de.joesst.dev:queue-ti-java-client:VERSION'
}
```

**Maven**

```xml
<repositories>
    <repository>
        <id>github-queue-ti</id>
        <url>https://maven.pkg.github.com/Joessst-Dev/queue-ti-java-client</url>
    </repository>
</repositories>

<dependency>
    <groupId>de.joesst.dev</groupId>
    <artifactId>queue-ti-java-client</artifactId>
    <version>VERSION</version>
</dependency>
```

### Local build (no token required)

To build and install directly from source:

```bash
git clone https://github.com/Joessst-Dev/queue-ti-java-client.git
cd queue-ti-java-client
./gradlew publishToMavenLocal
```

Then use `mavenLocal()` as the repository and `1.0-SNAPSHOT` as the version (the default when no `-PreleaseVersion` is passed to Gradle).

## Quick Start

All examples assume the following import:

```java
import de.joesst.dev.queueti.*;
```

### Connect to the server

```java
try (var client = QueueTiClient.connect("localhost:50051",
        ConnectOptions.builder().insecure(true).build())) {
    // use client...
}
```

For TLS with system CAs, omit both `.insecure(true)` and `.tls(...)` — the client uses TLS with the JVM's default trust store automatically.

`QueueTiClient` implements `Closeable`. Always close it (or use try-with-resources) to stop the background token-refresher thread and drain in-flight RPCs cleanly.

### TLS configuration

Use `TlsOptions` to configure custom CAs, mutual TLS, or server name override. `TlsOptions` is mutually exclusive with `.insecure(true)`.

**Custom CA (self-signed server):**

```java
byte[] caPem = Files.readAllBytes(Path.of("/path/to/ca.pem"));

try (var client = QueueTiClient.connect("myserver:50051",
        ConnectOptions.builder()
                .tls(TlsOptions.builder()
                        .rootCertificates(caPem)
                        .build())
                .build())) {
    // ...
}
```

**Mutual TLS (mTLS):**

```java
byte[] caPem   = Files.readAllBytes(Path.of("ca.pem"));
byte[] keyPem  = Files.readAllBytes(Path.of("client-key.pem"));
byte[] certPem = Files.readAllBytes(Path.of("client-cert.pem"));

try (var client = QueueTiClient.connect("myserver:50051",
        ConnectOptions.builder()
                .tls(TlsOptions.builder()
                        .rootCertificates(caPem)
                        .privateKey(keyPem)
                        .certificateChain(certPem)
                        .build())
                .build())) {
    // ...
}
```

**Server name override** (certificate hostname does not match the dial address):

```java
try (var client = QueueTiClient.connect("localhost:50051",
        ConnectOptions.builder()
                .tls(TlsOptions.builder()
                        .rootCertificates(caPem)
                        .serverNameOverride("myserver.internal")
                        .build())
                .build())) {
    // ...
}
```

### TlsOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rootCertificates` | `byte[]` | `null` | PEM CA cert(s) to trust; `null` uses JVM default trust store |
| `privateKey` | `byte[]` | `null` | PEM client private key for mTLS |
| `certificateChain` | `byte[]` | `null` | PEM client certificate chain for mTLS |
| `serverNameOverride` | `String` | `null` | Override hostname for SNI and certificate verification |

`privateKey` and `certificateChain` must be either both set (mTLS) or both `null`.

### Publish a message

```java
var producer = client.newProducer();
String messageId = producer.publish("my-topic", "Hello".getBytes()).get();
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
    process(message.payload());
    return null;  // Void return type — null signals success (ack); throw any exception to nack
});
```

`consume()` blocks until the calling thread is interrupted. Messages are dispatched on virtual threads; concurrency is bounded by the configured level. Automatic exponential-backoff reconnection (500ms–30s) handles stream failures.

### Consume messages (batch polling)

```java
consumer.consumeBatch(10, messages -> {
    for (var msg : messages) {
        if (isPoisonPill(msg)) {
            msg.nack("unprocessable").join();  // explicitly nack before returning
        } else {
            process(msg.payload());
        }
    }
    return null;  // any message not explicitly settled is auto-acked on normal return
                  // throw an exception to nack all unsettled messages instead
});
```

Individual messages can be acked or nacked within the handler before it returns. On normal return, any message not yet explicitly settled is auto-acked. On throw, any message not yet nacked is nacked with the exception message as the reason.

## Configuration

### ConnectOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `insecure` | `boolean` | `false` | Use plaintext channel (no TLS) |
| `token` | `String` | `null` | Initial JWT to send on every request |
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

### BatchOptions

`consumeBatch` accepts an optional `BatchOptions` argument to override per-call consumer group and visibility timeout independently of the `ConsumerOptions` set at construction:

```java
var batchOptions = BatchOptions.builder()
    .consumerGroup("batch-group")
    .visibilityTimeoutSeconds(30)
    .build();
consumer.consumeBatch(10, handler, batchOptions);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `consumerGroup` | `String` | `""` | Consumer group name for this batch poll |
| `visibilityTimeoutSeconds` | `Integer` | `null` | Visibility timeout for this batch poll; `null` uses server default |

## Token Refresh

If you need dynamic token renewal (e.g., from OAuth2 or a token service), configure a `TokenRefresher`. An initial `token` is required — the refresher is triggered by parsing the expiry of the current token, so without one it will never fire:

```java
TokenRefresher refresher = () -> myAuthClient.fetchToken();  // returns CompletableFuture<String>

var options = ConnectOptions.builder()
    .token(initialJwt)           // required — the refresher won't fire without a parseable token
    .tokenRefresher(refresher)
    .build();

try (var client = QueueTiClient.connect("localhost:50051", options)) {
    // ...
}
```

The client launches a background virtual thread that wakes 60 seconds before token expiry and calls your refresher. On failure it retries with exponential backoff (5s–60s). Each refresh call has a 30-second timeout.

You can also update the token at any time:

```java
client.setToken(newToken);
```

## Message

Every consumed message exposes:

| Field | Type | Description |
|-------|------|-------------|
| `id()` | `String` | Unique server-assigned message ID |
| `topic()` | `String` | Topic the message was published to |
| `payload()` | `byte[]` | Raw message bytes (defensive copy on each call) |
| `metadata()` | `Map<String, String>` | Immutable metadata map |
| `createdAt()` | `Instant` | Wall-clock enqueue time; `Instant.EPOCH` if not set by server |
| `retryCount()` | `int` | Number of previous delivery attempts (0 on first delivery) |
| `maxRetries()` | `OptionalInt` | Server-configured max retries (batch polling only; empty for streaming) |

Acknowledge or negative-acknowledge explicitly:

```java
message.ack()                           // success
message.nack("processing error")        // failure with reason
```

Both return `CompletableFuture<Void>` that completes when the server confirms.

## Admin Client

`AdminClient` manages topics, schemas, consumer groups, and server stats via the queue-ti HTTP admin API. It is separate from `QueueTiClient` so queue-only consumers carry no extra dependency surface.

```java
// No auth (local / dev)
var admin = AdminClient.connect("http://localhost:8080", AdminOptions.defaults());

// With bearer token
var admin = AdminClient.connect("http://localhost:8080",
        AdminOptions.builder().token("eyJ...").build());
```

### Topic configuration

```java
// List all topic configs
List<TopicConfig> configs = admin.listTopicConfigs();

// Create or replace a topic config (null fields use server defaults)
admin.upsertTopicConfig("orders", new TopicConfig(
        "orders",
        true,   // replayable
        3,      // maxRetries
        null, null, null, null));

// Delete
admin.deleteTopicConfig("orders");
```

### Schema management

```java
List<TopicSchema> schemas = admin.listTopicSchemas();
TopicSchema schema = admin.getTopicSchema("orders");   // throws NotFoundException on 404

admin.upsertTopicSchema("orders", "{\"type\":\"string\"}");
admin.deleteTopicSchema("orders");
```

| Field | Type | Description |
|-------|------|-------------|
| `topic` | `String` | Topic name |
| `schemaJson` | `String` | The JSON Schema document |
| `version` | `int` | Schema version number |
| `updatedAt` | `String` | ISO-8601 timestamp of the last update |

### Consumer groups

```java
List<String> groups = admin.listConsumerGroups("orders");

admin.registerConsumerGroup("orders", "billing");     // throws ConflictException if already exists
admin.unregisterConsumerGroup("orders", "billing");   // throws NotFoundException if not found
```

### Stats

```java
List<TopicStat> stats = admin.stats();
// each entry: stat.topic(), stat.status(), stat.count()
```

### AdminOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `token` | `String` | `null` | Bearer token sent in every `Authorization` header |
| `requestTimeout` | `Duration` | 30s | Per-request HTTP timeout (must be positive) |

### Exceptions

| Exception | HTTP status | Meaning |
|-----------|-------------|---------|
| `NotFoundException` | 404 | Resource does not exist |
| `ConflictException` | 409 | Resource already exists |
| `UncheckedIOException` | other / network | Unexpected error |

### TopicConfig fields

| Field | Type | Description |
|-------|------|-------------|
| `topic` | `String` | Topic name |
| `replayable` | `boolean` | Whether the topic supports message replay |
| `maxRetries` | `Integer` | Max delivery attempts; `null` = server default |
| `messageTtlSeconds` | `Integer` | Message TTL in seconds; `null` = server default |
| `maxDepth` | `Integer` | Max queue depth; `null` = server default |
| `replayWindowSeconds` | `Integer` | Replay window in seconds; `null` = server default |
| `throughputLimit` | `Integer` | Max messages per second; `null` = server default |

## Building from Source

```bash
./gradlew build                         # compile + all tests
./gradlew test                          # tests only
./gradlew clean                         # remove build outputs
./gradlew generateProto                 # regenerate gRPC stubs from proto
./gradlew publishToMavenLocal           # install to local Maven cache (~/.m2)
```

Tests use JUnit 5 with an in-process gRPC server — no external server or mocks needed.

## Related

- **queue-ti server**: https://github.com/Joessst-Dev/queue-ti — the server implementation, proto schema, and reference clients in Go, Node.js, and Python
