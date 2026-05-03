---
name: queue-ti-java-client
description: Use when implementing, extending, or debugging the queue-ti Java client library. Covers the full API surface (Client, Producer, Consumer, Message, TokenStore), gRPC/protobuf wiring, and Java idioms matching the Go/Node.js/Python reference clients.
license: MIT
metadata:
  author: jostweyers@gmail.com
  version: "1.0.0"
  domain: library
  triggers: queue-ti, QueueTiClient, Producer, Consumer, TokenStore, gRPC client, message queue client
  role: implementer
  scope: implementation
  output-format: code
  related-skills: java-library-engineer, java-cicd-engineer
---

# queue-ti Java Client

Java implementation of the queue-ti gRPC client library, mirroring the Go, Node.js, and Python reference clients.

## Reference implementations

Before writing Java code, consult the reference implementations:
- Go client: `clients/go-client/` in the `Joessst-Dev/queue-ti` repo (canonical reference)
- Node.js client: `clients/node/src/` (async patterns)
- Python client: `clients/python/queueti/` (sync + async patterns)

Load the API spec before implementing any component:
`references/api-spec.md`

## Core Workflow

1. **Proto first** — generate Java stubs from `proto/queue.proto` using the protobuf Gradle plugin (`references/grpc-java-setup.md`). Never hand-write proto stubs.
2. **TokenStore** — thread-safe, JWT expiry parsing. Implement before `Client`.
3. **Client** — wraps the gRPC `ManagedChannel`, owns the `TokenStore`, runs background token refresh. Implements `Closeable`.
4. **Producer** — stateless; delegates to `QueueServiceGrpc` stub. Returns `CompletableFuture<String>` (message ID).
5. **Consumer** — `consume(MessageHandler)` for streaming (Subscribe RPC), `consumeBatch(int, BatchMessageHandler)` for polling (BatchDequeue RPC). Both block until cancellation or error.
6. **Message** — value class; `ack()` and `nack(String)` wired back to the server via closures captured at construction.
7. **Tests** — use an in-process gRPC server (`InProcessChannelBuilder`) rather than mocks. See `references/testing-patterns.md` in the java-architect skill.

After each component: run `./gradlew test` and confirm it passes before moving on.

## API Surface

### Client

```java
// Connect (insecure, suitable for local/dev)
QueueTiClient client = QueueTiClient.connect("localhost:50051",
    ConnectOptions.builder().insecure(true).build());

// Connect with JWT auth
QueueTiClient client = QueueTiClient.connect("localhost:50051",
    ConnectOptions.builder()
        .token("eyJ...")
        .tokenRefresher(() -> CompletableFuture.completedFuture(fetchNewToken()))
        .build());

client.newProducer();                          // → Producer
client.newConsumer("my-topic");                // → Consumer (default options)
client.newConsumer("my-topic", ConsumerOptions.builder().concurrency(4).build());
client.setToken("eyJ...");                     // update JWT without reconnect
client.close();                                // stop refresher, close channel
```

### Producer

```java
Producer producer = client.newProducer();

// Publish — returns assigned message ID
String id = producer.publish("orders", payload).get();

// With options
String id = producer.publish("orders", payload,
    PublishOptions.builder()
        .metadata(Map.of("source", "checkout"))
        .key("order-42")                       // dedup key
        .build()).get();
```

### Consumer — streaming

```java
Consumer consumer = client.newConsumer("orders");

// Blocks until context cancelled; auto-reconnects with backoff (500ms→30s)
consumer.consume(ctx, msg -> {
    process(msg.payload());
    return null;  // null → Ack; throw → Nack with exception message
});
```

### Consumer — batch polling

```java
consumer.consumeBatch(ctx, 10, (ctx2, messages) -> {
    messages.forEach(m -> process(m));
    return null;
});
```

### Message

```java
msg.id()           // String
msg.topic()        // String
msg.payload()      // byte[]
msg.metadata()     // Map<String, String>
msg.createdAt()    // Instant
msg.retryCount()   // int
msg.ack(ctx)       // CompletableFuture<Void>
msg.nack(ctx, "reason")  // CompletableFuture<Void>
```

## Key Design Decisions

| Concern | Decision | Reason |
|---------|----------|--------|
| Async API | `CompletableFuture<T>` | Standard Java async, no extra dependency |
| Options | Immutable builders | Matches Go functional-options ergonomics in Java style |
| Handler return | `null` → Ack, throw → Nack | Mirrors Go `HandlerFunc`; simpler than checked exceptions |
| Token refresh | `ScheduledExecutorService` daemon thread | Matches Go goroutine / Node.js async loop |
| Concurrency in consumer | `Semaphore` + virtual threads (Java 21) | Matches Go channel semaphore pattern |
| Channel credentials | `CallCredentials` subclass reading from `TokenStore` | Matches Go `PerRPCCredentials` / Node.js `CallCredentials.createFromMetadataGenerator` |
| Backoff constants | `BACKOFF_START = 500ms`, `BACKOFF_MAX = 30s` | Same as all reference clients |
| Token refresh advance window | 60 seconds before expiry | Same as Go and Node.js clients |

## Constraints

### MUST DO
- Generate proto stubs from the canonical `proto/queue.proto` (do not copy from another client)
- Use Java 21 LTS (records for value types, virtual threads for concurrency)
- `TokenStore` must be thread-safe (`ReentrantReadWriteLock` or `AtomicReference`)
- `Client` implements `Closeable`; `close()` must stop the background refresher and close the channel
- Handle `null` `created_at` timestamps gracefully (use `Instant.EPOCH` as fallback)
- Forward `consumerGroup` on both `AckRequest` and `NackRequest`
- Recover handler panics (unchecked exceptions) as Nacks in `Consumer`
- All public API methods that may throw are documented with `@throws`

### MUST NOT DO
- Do not use blocking gRPC stubs (`QueueServiceGrpc.newBlockingStub`) in the consumer's Subscribe loop — use async stubs or the streaming stub
- Do not skip the `consumerGroup` field on requests even when empty — send it as an empty string (server uses it to distinguish legacy behaviour)
- Do not hardcode addresses or tokens
- Do not swallow `InterruptedException` — restore the interrupt flag

## Reference Files

| Topic | File | Load When |
|-------|------|-----------|
| Proto schema + full RPC spec | `references/api-spec.md` | Starting any component |
| Gradle + protobuf plugin setup | `references/grpc-java-setup.md` | Setting up the build or adding proto compilation |
| Java idiom patterns | `references/java-patterns.md` | Implementing Client, TokenStore, Consumer concurrency |
