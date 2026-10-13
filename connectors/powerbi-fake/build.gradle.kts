// A fake Power BI for tests and demos: an Entra token endpoint and the push
// dataset REST API, answered from memory. Never part of the production image.
plugins {
    `java-library`
    application
}

val jacksonVersion: String by project

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
}

application {
    mainClass.set("dev.turgon.connector.powerbi.fake.FakePowerBi")
    applicationName = "turgon-fake-powerbi"
}
