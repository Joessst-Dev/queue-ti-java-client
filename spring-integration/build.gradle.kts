plugins {
    id("java-library")
    id("maven-publish")
}

group = "de.joesst.dev"
version = findProperty("releaseVersion")?.toString() ?: "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

val springIntegrationVersion = "6.4.4"
val grpcVersion = "1.68.0"
val protobufVersion = "4.28.2"

dependencies {
    api(project(":"))

    implementation("org.springframework.integration:spring-integration-core:$springIntegrationVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.springframework.integration:spring-integration-test:$springIntegrationVersion")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")
    testImplementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    testImplementation("io.grpc:grpc-stub:$grpcVersion")
    testImplementation("io.grpc:grpc-protobuf:$grpcVersion")
    testImplementation("com.google.protobuf:protobuf-java:$protobufVersion")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "queue-ti-spring-integration"
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/Joessst-Dev/queue-ti-java-client")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
}
