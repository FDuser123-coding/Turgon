import com.google.protobuf.gradle.id

plugins {
    `java-library`
    id("com.google.protobuf")
}

val grpcVersion: String by project
val protobufVersion: String by project
val camelVersion: String by project
val jacksonVersion: String by project

dependencies {
    api("io.grpc:grpc-stub:$grpcVersion")
    api("io.grpc:grpc-protobuf:$grpcVersion")
    api("com.google.protobuf:protobuf-java:$protobufVersion")
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    api("org.apache.camel:camel-core-engine:$camelVersion")
    api("org.apache.camel:camel-direct:$camelVersion")
    api("org.apache.camel:camel-core-languages:$camelVersion")
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
}

// The protocol is defined once, at the repository's root.
sourceSets { main { proto { srcDir("../../proto") } } }

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins { id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" } }
    generateProtoTasks { all().forEach { it.plugins { id("grpc") } } }
}

// Generated code is not ours to lint.
tasks.named<JavaCompile>("compileJava") {
    options.compilerArgs.removeAll(listOf("-Werror"))
    options.compilerArgs.add("-Xlint:-rawtypes,-unchecked,-cast,-dep-ann")
}
