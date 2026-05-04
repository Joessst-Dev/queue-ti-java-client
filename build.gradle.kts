import com.google.protobuf.gradle.id

plugins {
    id("java")
    id("maven-publish")
    id("com.google.protobuf") version "0.9.4"
}

group = "de.joesst.dev"
version = "1.0-SNAPSHOT"

val grpcVersion = "1.68.0"
val protobufVersion = "4.28.2"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-core:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")
    implementation("com.google.guava:guava:33.3.1-jre")

    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testImplementation("org.assertj:assertj-core:3.26.3")
}

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins {
        create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins { create("grpc") }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }

    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/Joessst-Dev/queue-ti-java-client")
            credentials {
                // GITHUB_TOKEN is injected by the release workflow via the
                // GITHUB_TOKEN secret; locally this block is a no-op unless
                // the env vars are explicitly set.
                username = System.getenv("GITHUB_ACTOR") ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
}
