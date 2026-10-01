// A fake ECC for tests and demos: BAPIs answered from memory. Never part
// of the production image; the demo distribution adds it to the class path.
plugins {
    `java-library`
    application
}

dependencies {
    api(project(":sap-ecc"))
}

application {
    mainClass.set("dev.turgon.connector.sdk.ConnectorServer")
    applicationName = "turgon-connector-sap-ecc-demo"
}
