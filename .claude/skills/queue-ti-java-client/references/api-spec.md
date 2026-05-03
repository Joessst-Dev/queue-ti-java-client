# queue-ti API Specification

Source: `proto/queue.proto` in `Joessst-Dev/queue-ti`.

## Protobuf Schema

```protobuf
syntax = "proto3";
package queue;
option java_package = "de.joesst.dev.queueti.pb";
option java_outer_classname = "QueueProto";
option java_multiple_files = true;

import "google/protobuf/timestamp.proto";

service QueueService {
  rpc Enqueue(EnqueueRequest)           returns (EnqueueResponse);
  rpc Dequeue(DequeueRequest)           returns (DequeueResponse);
  rpc BatchDequeue(BatchDequeueRequest) returns (BatchDequeueResponse);
  rpc Ack(AckRequest)                   returns (AckResponse);
  rpc Nack(NackRequest)                 returns (NackResponse);
  rpc Subscribe(SubscribeRequest)       returns (stream SubscribeResponse);
}

message EnqueueRequest {
  string topic   = 1;
  bytes  payload = 2;
  map<string, string> metadata = 3;
  optional string key = 4;
}
message EnqueueResponse { string id = 1; }

message DequeueRequest {
  string topic = 1;
  optional uint32 visibility_timeout_seconds = 2;
  string consumer_group = 3;
}
message DequeueResponse {
  string id          = 1;
  string topic       = 2;
  bytes  payload     = 3;
  map<string, string> metadata = 4;
  google.protobuf.Timestamp created_at = 5;
  int32 retry_count  = 6;
  int32 max_retries  = 7;
  optional string key = 8;
}

message BatchDequeueRequest {
  string topic  = 1;
  uint32 count  = 2;
  optional uint32 visibility_timeout_seconds = 3;
  string consumer_group = 4;
}
message BatchDequeueResponse { repeated DequeueResponse messages = 1; }

message AckRequest  { string id = 1; string consumer_group = 2; }
message AckResponse {}

message NackRequest { string id = 1; string error = 2; string consumer_group = 3; }
message NackResponse {}

message SubscribeRequest {
  string topic = 1;
  optional uint32 visibility_timeout_seconds = 2;
  string consumer_group = 3;
}
message SubscribeResponse {
  string id          = 1;
  string topic       = 2;
  bytes  payload     = 3;
  map<string, string> metadata = 4;
  google.protobuf.Timestamp created_at = 5;
  int32 retry_count  = 6;
}
```

## Java package options to add at compile time

The canonical `.proto` file does not include Java options. Add them via the protobuf Gradle plugin's `descriptor` or by patching the generated sources — or add the options in a local copy of the proto placed under `src/main/proto/`:

```protobuf
option java_package = "de.joesst.dev.queueti.pb";
option java_outer_classname = "QueueProto";
option java_multiple_files = true;
```

## RPC Behaviour Notes

### Enqueue
- `key` is optional; when provided the server upserts instead of inserting a new row.
- Returns the server-assigned UUID string `id`.

### Subscribe (streaming)
- Server pushes messages as a `ServerStreamingRpc`. The stream stays open until the client cancels or the server closes it.
- On stream error or EOF the client must reconnect with exponential backoff (500ms → 30s).
- `SubscribeResponse` does **not** include `max_retries` or `key` (unlike `DequeueResponse`).

### BatchDequeue (polling)
- Returns up to `count` messages atomically.
- When the response list is empty apply backoff (500ms → 30s); reset on non-empty response.

### Ack / Nack
- `consumer_group` must be forwarded on both. An empty string (`""`) uses legacy single-consumer behaviour.
- Nack stores `error` as the failure reason and makes the message visible again after the visibility timeout.

### Auth
- All RPCs accept a JWT `Bearer` token in the `authorization` gRPC metadata header.
- Token is attached via a `CallCredentials` that reads from a shared `TokenStore`.
- Background refresher calls the `TokenRefresher` callback ~60 seconds before the JWT `exp` claim.
- Backoff for failed refreshes: start 5s, double, cap 60s.
