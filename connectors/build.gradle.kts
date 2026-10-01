plugins {
    java
    id("com.google.protobuf") version "0.9.5" apply false
}

allprojects {
    group = "dev.turgon.connectors"
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "java")
    java { toolchain { languageVersion.set(JavaLanguageVersion.of(21)) } }
    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
        options.compilerArgs.addAll(listOf("-Xlint:all,-processing,-serial,-try", "-Werror"))
    }
    dependencies {
        "testImplementation"(platform("org.junit:junit-bom:5.13.4"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging { events("failed"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
    }
}
