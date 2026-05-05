# Order Pipeline — Java

Demonstrates the full producer → consumer → ack lifecycle using the queue-ti Java client.

## What it does

1. Registers the `fulfillment` consumer group via the admin REST API (ignores 409 if it already exists)
2. Publishes 5 orders to the `orders` topic — one is a poison pill that will be nacked
3. Consumes the topic with 3 concurrent handlers — valid orders are acked, the poison pill is nacked and eventually dead-lettered
4. Drains the `orders.dlq` topic so dead-lettered messages are visible

## Prerequisites

- Java 21+
- queue-ti running locally: `docker-compose up` from the repo root

## Run

From the repository root:

```bash
./gradlew :examples:order-pipeline:run
```

Press **Ctrl-C** to stop the consumer.

## Configuration

Edit the constants at the top of `OrderPipeline.java` to point at a different server:

| Constant | Default | Description |
|----------|---------|-------------|
| `GRPC_ADDR` | `localhost:50051` | gRPC server address |
| `ADMIN_ADDR` | `http://localhost:8080` | HTTP admin API base URL |
| `TOPIC` | `orders` | Topic to publish and consume |
| `DLQ_TOPIC` | `orders.dlq` | Dead-letter queue topic |
| `CONSUMER_GROUP` | `fulfillment` | Consumer group name |
