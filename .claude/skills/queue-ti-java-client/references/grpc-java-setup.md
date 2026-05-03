# gRPC Java Setup (Gradle Kotlin DSL)

## Required Dependencies

Add to `build.gradle.kts`:

```kotlin
plugins {
    id("java")
    id("com.google.protobuf") version "0.9.4"
}

group = "de.joesst.dev"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val grpcVersion = "1.68.0"
val protobufVersion = "4.28.2"

dependencies {
    // gRPC runtime
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-core:$grpcVersion")

    // Protobuf + well-known types (Timestamp)
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    // Required by generated stubs on Java 9+
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    // Tests
    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")   // in-process test server
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
```

## Proto File Location

Place the proto file at:

```
src/main/proto/queue.proto
```

The plugin generates Java sources into `build/generated/source/proto/main/java/` and `build/generated/source/proto/main/grpc/`. These are automatically added to the compile classpath.

## Syncing the Proto File

The canonical proto lives at `proto/queue.proto` in the `Joessst-Dev/queue-ti` monorepo. Copy it into `src/main/proto/queue.proto` and add the Java-specific options at the top:

```protobuf
syntax = "proto3";
package queue;

option java_package = "de.joesst.dev.queueti.pb";
option java_outer_classname = "QueueProto";
option java_multiple_files = true;

import "google/protobuf/timestamp.proto";
// ... rest of the proto unchanged
```

## Verifying the Build

```bash
./gradlew generateProto        # generates stubs only
./gradlew compileJava          # compiles everything including generated sources
./gradlew build                # full build + tests
```

Generated classes you will use:
- `de.joesst.dev.queueti.pb.QueueServiceGrpc` — service stub factory
- `de.joesst.dev.queueti.pb.QueueServiceGrpc.QueueServiceStub` — async stub (for Subscribe)
- `de.joesst.dev.queueti.pb.QueueServiceGrpc.QueueServiceFutureStub` — future stub (for Enqueue, Ack, Nack, BatchDequeue)
- `de.joesst.dev.queueti.pb.EnqueueRequest`, `SubscribeRequest`, etc.

## CallCredentials for JWT Auth

```java
public final class BearerTokenCredentials extends CallCredentials {

    private final TokenStore store;

    public BearerTokenCredentials(TokenStore store) {
        this.store = store;
    }

    @Override
    public void applyRequestMetadata(
            RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        appExecutor.execute(() -> {
            Metadata headers = new Metadata();
            headers.put(
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + store.get());
            applier.apply(headers);
        });
    }
}
```

Attach during channel build:

```java
ManagedChannel channel = NettyChannelBuilder.forTarget(address)
    .usePlaintext()   // insecure; omit for TLS
    .build();

// Wrap stub with credentials
QueueServiceGrpc.QueueServiceFutureStub futureStub =
    QueueServiceGrpc.newFutureStub(channel)
        .withCallCredentials(new BearerTokenCredentials(tokenStore));

QueueServiceGrpc.QueueServiceStub asyncStub =
    QueueServiceGrpc.newStub(channel)
        .withCallCredentials(new BearerTokenCredentials(tokenStore));
```
