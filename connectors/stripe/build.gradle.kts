plugins {
    `java-library`
    application
}

dependencies {
    api(project(":sdk"))
    testImplementation(project(":stripe-fake"))
}

application {
    mainClass.set("dev.turgon.connector.sdk.ConnectorServer")
    applicationName = "turgon-connector-stripe"
}
