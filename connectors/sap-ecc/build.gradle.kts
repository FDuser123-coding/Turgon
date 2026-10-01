plugins {
    `java-library`
    application
}

dependencies {
    api(project(":sdk"))
    testImplementation(project(":sap-ecc-fake"))
}

application {
    mainClass.set("dev.turgon.connector.sdk.ConnectorServer")
    applicationName = "turgon-connector-sap-ecc"
}

// SAP JCo is licensed by SAP and not redistributable. The image loads it
// from /opt/turgon/lib; outside it, the start script takes lib/sapjco3.jar
// if present (and JAVA_OPTS=-Djava.library.path=<dir of libsapjco3.so>).
tasks.named<CreateStartScripts>("startScripts") {
    classpath = classpath!! + files("sapjco3.jar")
}
