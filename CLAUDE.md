# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Java client library for the [queue-ti](https://github.com/Joessst-Dev/queue-ti) gRPC message queue service.

- **Group / artifact**: `de.joesst.dev` / `queue-ti-java-client`
- **Java**: 21 LTS
- **Build**: Gradle 8 (Kotlin DSL)
- **Tests**: JUnit 5 Jupiter + in-process gRPC server (no mocks)
- **Skill**: `/queue-ti-java-client` — load it before implementing anything

## Commands

```bash
./gradlew build                                                     # compile + all tests
./gradlew test                                                      # tests only
./gradlew test --tests "de.joesst.dev.queueti.ProducerTest"        # single class
./gradlew test --tests "de.joesst.dev.queueti.ProducerTest.publish_returnsId"  # single method
./gradlew generateProto                                             # regenerate gRPC/proto stubs
./gradlew clean                                                     # remove build outputs
```

## Package Layout

```
de.joesst.dev.queueti
├── QueueTiClient            public entry point — connect(), newProducer(), newConsumer(), close()
├── Producer                 publish() → CompletableFuture<String>
├── Consumer                 consume() streaming, consumeBatch() polling
├── Message                  value class with ack() / nack()
├── MessageHandler           @FunctionalInterface — null→Ack, throw→Nack
├── BatchMessageHandler      @FunctionalInterface for batch consume
├── TokenRefresher           @FunctionalInterface — () → CompletableFuture<String>
├── ConnectOptions           immutable builder
├── ConsumerOptions          immutable builder
├── PublishOptions           immutable builder
├── BatchOptions             immutable builder
├── TokenStore               package-private — thread-safe JWT container + expiry parsing
└── BearerTokenCredentials   package-private — CallCredentials for JWT auth
```

Generated proto stubs land in `de.joesst.dev.queueti.pb` (via `build/generated/source/proto/`).

## Implementation Order

Follow this sequence — each step must pass `./gradlew test` before the next begins:

1. **Build setup** — update `build.gradle.kts`: protobuf plugin, gRPC/protobuf dependencies, `java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }`
2. **Proto file** — `src/main/proto/queue.proto` (canonical source: `Joessst-Dev/queue-ti/proto/queue.proto` + Java options); run `generateProto` to verify
3. **TokenStore** — thread-safe get/set, `parseExpiry(String) → Instant`, test with valid/malformed JWTs
4. **Options** — four immutable builder classes (`ConnectOptions`, `PublishOptions`, `ConsumerOptions`, `BatchOptions`), no logic, test construction
5. **BearerTokenCredentials** — `CallCredentials` subclass reading from `TokenStore`
6. **Client** — `QueueTiClient.connect(address, options)`, owns `ManagedChannel`, optional background token refresher (virtual thread), `newProducer()`, `newConsumer()`, `setToken()`, `close()`; test with in-process channel
7. **Message** — value class; `ack()` / `nack(reason)` wired via constructor-injected functions; test ack/nack delegation
8. **Producer** — `publish(topic, payload)` and `publish(topic, payload, PublishOptions)`; returns `CompletableFuture<String>`; test with fake `Enqueue` impl
9. **Consumer (streaming)** — `consume(handler)` via Subscribe RPC, semaphore concurrency, exponential backoff reconnect (500ms→30s), panic→Nack; test with fake streaming server
10. **Consumer (batch)** — `consumeBatch(batchSize, handler)` via BatchDequeue, empty-queue backoff, reset on messages; test with fake BatchDequeue impl

## Key Invariants

- `consumerGroup` is sent on every `AckRequest`, `NackRequest`, `SubscribeRequest`, `BatchDequeueRequest` — empty string is valid (legacy behaviour)
- Null `created_at` timestamps → `Instant.EPOCH`
- Background token refresher: wake 60s before JWT `exp`; retry backoff 5s→60s
- Never use `QueueServiceGrpc.newBlockingStub` in the Subscribe consumer loop
- `InterruptedException` must restore the interrupt flag, never swallowed

## Reference Clients

The Go client (`clients/go-client/`) is the canonical reference. Node.js for async patterns.
Repo: `https://github.com/Joessst-Dev/queue-ti`
