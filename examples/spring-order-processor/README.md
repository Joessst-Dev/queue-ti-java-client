# spring-order-processor

A Spring Boot application that demonstrates the `queue-ti-spring-boot-starter` and `queue-ti-spring-integration` modules working together.

## What it does

1. **Publisher** — an `ApplicationRunner` injects the auto-configured `Producer` bean and publishes five sample orders to the `orders` topic on startup. One of the five is a poison pill.
2. **Consumer** — an `IntegrationFlow` wires a `QueueTiInboundChannelAdapter` (MANUAL acknowledge mode) to a `DirectChannel`. The flow decodes the JSON payload, acks valid orders, and nacks the poison pill with an explicit rejection reason.

```
ApplicationRunner  ──publishes──▶  orders topic
                                       │
QueueTiInboundChannelAdapter ◀─────────┘
        │
        ▼  DirectChannel
        │
   transform byte[] → String
        │
   handle  ──ack──▶  valid orders
           ──nack─▶  poison pills ("poison pill detected")
```

## Prerequisites

- Java 21
- queue-ti server running locally:

```bash
# from the queue-ti repo root
docker compose up
```

## Run

```bash
./gradlew :examples:spring-order-processor:bootRun
```

Expected output (order may vary due to concurrent processing):

```
Publishing 5 orders...
Published ord-1 → <id>
Published ord-2 → <id>
...
Fulfilling order <id>: {"id":"ord-1","item":"Widget A",...}
Fulfilling order <id>: {"id":"ord-2","item":"Gadget B",...}
Nacking poison pill <id>
Fulfilling order <id>: {"id":"ord-4","item":"Widget C",...}
Fulfilling order <id>: {"id":"ord-5","item":"Gadget D",...}
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `queueti.grpc-address` | `localhost:50051` | queue-ti gRPC endpoint |
| `queueti.insecure` | `true` | Plaintext channel (no TLS) |

Override via environment variable or `application.yml`:

```bash
QUEUETI_GRPC_ADDRESS=myserver:50051 ./gradlew :examples:spring-order-processor:bootRun
```
