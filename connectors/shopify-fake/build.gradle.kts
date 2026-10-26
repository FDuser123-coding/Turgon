// A fake Shopify for tests and demos: the GraphQL Admin API of one store,
// answered from memory. Never part of the production image.
plugins {
    `java-library`
    application
}

val jacksonVersion: String by project

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
}

application {
    mainClass.set("dev.turgon.connector.shopify.fake.FakeShopify")
    applicationName = "turgon-fake-shopify"
}
