# Java Idiom Patterns for queue-ti Client

## TokenStore — thread-safe JWT container

```java
public final class TokenStore {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private String token;

    public TokenStore(String initial) {
        this.token = initial;
    }

    public String get() {
        lock.readLock().lock();
        try { return token; }
        finally { lock.readLock().unlock(); }
    }

    public void set(String token) {
        lock.writeLock().lock();
        try { this.token = token; }
        finally { lock.writeLock().unlock(); }
    }

    /** Decodes JWT payload and returns the exp claim as Instant. */
    public static Instant parseExpiry(String token) {
        String[] parts = token.split("\\.");
        if (parts.length != 3) throw new IllegalArgumentException("Malformed JWT");
        byte[] payload = Base64.getUrlDecoder().decode(parts[1]);
        // parse {"exp": <unix seconds>} from JSON payload
        // Use minimal JSON parsing — avoid pulling in a full JSON library just for this
        String json = new String(payload, StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("\"exp\"\\s*:\\s*(\\d+)").matcher(json);
        if (!m.find()) throw new IllegalArgumentException("JWT has no exp claim");
        return Instant.ofEpochSecond(Long.parseLong(m.group(1)));
    }
}
```

## Background Token Refresher

Mirrors the Go `runRefresher` goroutine and Node.js `runRefresher` async loop.

```java
private void startRefresher(TokenRefresher refresher) {
    refresherThread = Thread.ofVirtual().name("queue-ti-token-refresher").start(() -> {
        Duration retryBackoff = Duration.ofSeconds(5);
        final Duration advanceWindow = Duration.ofSeconds(60);
        final Duration backoffMax = Duration.ofSeconds(60);

        while (!Thread.currentThread().isInterrupted()) {
            Duration sleepFor;
            try {
                Instant exp = TokenStore.parseExpiry(tokenStore.get());
                Duration remaining = Duration.between(Instant.now(), exp).minus(advanceWindow);
                if (remaining.isNegative() || remaining.isZero()) {
                    sleepFor = Duration.ZERO;
                } else {
                    sleepFor = remaining;
                    retryBackoff = Duration.ofSeconds(5); // reset on healthy token
                }
            } catch (Exception e) {
                logger.warning("queue-ti: token refresher: cannot parse token expiry: " + e.getMessage());
                sleepFor = retryBackoff;
            }

            if (!sleepFor.isZero()) {
                try { Thread.sleep(sleepFor); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }

            if (Thread.currentThread().isInterrupted()) return;

            try {
                String newToken = refresher.refresh().get();
                tokenStore.set(newToken);
                retryBackoff = Duration.ofSeconds(5);
                logger.fine("queue-ti: token refresher: token refreshed successfully");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                logger.warning("queue-ti: token refresher: refresh failed: " + e.getMessage());
                try { Thread.sleep(retryBackoff); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                Duration doubled = retryBackoff.multipliedBy(2);
                retryBackoff = doubled.compareTo(backoffMax) < 0 ? doubled : backoffMax;
            }
        }
    });
}
```

## Consumer Concurrency — Semaphore Pattern

Mirrors Go's `chan struct{}` semaphore and Node.js slot queue.

```java
// Virtual-thread pool limited by semaphore
Semaphore sem = new Semaphore(concurrency);
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

// Inside the stream data callback:
sem.acquire();   // blocks if all slots used
executor.submit(() -> {
    try {
        dispatch(msg, handler);
    } finally {
        sem.release();
    }
});
```

## Consumer Streaming — StreamObserver

Use `QueueServiceGrpc.newStub(channel).subscribe(request, observer)` for the Subscribe RPC.

```java
private void openStream(StreamObserver<SubscribeResponse> observer) {
    SubscribeRequest req = SubscribeRequest.newBuilder()
        .setTopic(topic)
        .setConsumerGroup(consumerGroup)
        .build();
    asyncStub.subscribe(req, observer);
}
```

Implement `StreamObserver<SubscribeResponse>`:

```java
new StreamObserver<>() {
    @Override public void onNext(SubscribeResponse resp) {
        // build Message and dispatch
    }

    @Override public void onError(Throwable t) {
        if (cancelled) return;
        logger.warning("queue-ti consumer: stream error (will reconnect): " + t.getMessage());
        scheduleReconnect(backoff);
        backoff = nextBackoff(backoff);
    }

    @Override public void onCompleted() {
        if (!cancelled) scheduleReconnect(Duration.ZERO);
    }
}
```

## Backoff Helper

```java
private static Duration nextBackoff(Duration current) {
    Duration doubled = current.multipliedBy(2);
    return doubled.compareTo(BACKOFF_MAX) < 0 ? doubled : BACKOFF_MAX;
}

static final Duration BACKOFF_START = Duration.ofMillis(500);
static final Duration BACKOFF_MAX   = Duration.ofSeconds(30);
```

## Options — Builder Pattern

Use Java records for immutable option objects where possible (Java 21).

```java
public record ConnectOptions(
    boolean insecure,
    String token,               // nullable
    TokenRefresher tokenRefresher  // nullable
) {
    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private boolean insecure;
        private String token;
        private TokenRefresher tokenRefresher;

        public Builder insecure(boolean v)               { this.insecure = v; return this; }
        public Builder token(String t)                   { this.token = t; return this; }
        public Builder tokenRefresher(TokenRefresher r)  { this.tokenRefresher = r; return this; }

        public ConnectOptions build() {
            return new ConnectOptions(insecure, token, tokenRefresher);
        }
    }
}
```

## Functional Interfaces

```java
@FunctionalInterface
public interface MessageHandler {
    /** Return null to Ack; throw any exception to Nack. */
    Void handle(Message message) throws Exception;
}

@FunctionalInterface
public interface BatchMessageHandler {
    Void handle(List<Message> messages) throws Exception;
}

@FunctionalInterface
public interface TokenRefresher {
    CompletableFuture<String> refresh();
}
```

## Message — Record + Closures

```java
public final class Message {

    private final String id;
    private final String topic;
    private final byte[] payload;
    private final Map<String, String> metadata;
    private final Instant createdAt;
    private final int retryCount;

    // Wired at construction time; not exposed
    private final Function<Context, CompletableFuture<Void>> ackFn;
    private final BiFunction<Context, String, CompletableFuture<Void>> nackFn;

    // ... constructor, getters ...

    public CompletableFuture<Void> ack()                    { return ackFn.apply(null); }
    public CompletableFuture<Void> nack(String reason)      { return nackFn.apply(null, reason); }
}
```

## Dispatch — Exception → Nack

```java
private void dispatch(Message msg, MessageHandler handler) {
    try {
        handler.handle(msg);
        msg.ack().whenComplete((v, err) -> {
            if (err != null && !cancelled) {
                logger.warning("queue-ti consumer: ack failed for " + msg.id() + ": " + err);
            }
        });
    } catch (Throwable t) {
        msg.nack(t.getMessage() != null ? t.getMessage() : t.getClass().getName())
           .whenComplete((v, err) -> {
               if (err != null && !cancelled) {
                   logger.warning("queue-ti consumer: nack failed for " + msg.id() + ": " + err);
               }
           });
    }
}
```

## In-process gRPC Test Server

```java
class ClientTest {
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new FakeQueueService())
            .build()
            .start();
        channel = InProcessChannelBuilder.forName(serverName)
            .directExecutor()
            .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
    }
}
```
