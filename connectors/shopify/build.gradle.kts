plugins {
    `java-library`
    application
}

dependencies {
    api(project(":sdk"))
    testImplementation(project(":shopify-fake"))
}

application {
    mainClass.set("dev.turgon.connector.sdk.ConnectorServer")
    applicationName = "turgon-connector-shopify"
}
