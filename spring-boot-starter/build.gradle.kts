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

val springBootVersion = "3.4.5"

dependencies {
    // Exposes the library and its transitive deps (gRPC, protobuf, Gson) to consumers.
    api(project(":"))

    implementation("org.springframework.boot:spring-boot-autoconfigure:$springBootVersion")
    // Generates spring-configuration-metadata.json for IDE completion and validation.
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:$springBootVersion")

    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.springframework.boot:spring-boot-test-autoconfigure:$springBootVersion")
    testImplementation("org.assertj:assertj-core:3.26.3")
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "queue-ti-spring-boot-starter"
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
