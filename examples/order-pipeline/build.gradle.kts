plugins {
    id("java")
    id("application")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(project(":"))
}

application {
    mainClass = "de.joesst.dev.queueti.examples.orderpipeline.OrderPipeline"
}

tasks.run.configure {
    standardInput = System.`in`
}
