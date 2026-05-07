plugins {
    id("java")
    id("org.springframework.boot") version "3.4.5"
}

group = "de.joesst.dev"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

val springBootVersion = "3.4.5"
val springIntegrationVersion = "6.4.4"

dependencies {
    // Auto-configures QueueTiClient and Producer from application.yml
    implementation(project(":spring-boot-starter"))

    // QueueTiInboundChannelAdapter for wiring topics into IntegrationFlow pipelines
    implementation(project(":spring-integration"))

    // Spring Boot runtime — SpringApplication, logging, context lifecycle
    implementation("org.springframework.boot:spring-boot-starter:$springBootVersion")

    // Spring Integration DSL — IntegrationFlow, transform, handle
    implementation("org.springframework.integration:spring-integration-core:$springIntegrationVersion")
}
