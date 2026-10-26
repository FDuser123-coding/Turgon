// A fake Stripe for tests and demos: one account's customers, invoices and
// events, answered from memory. Never part of the production image.
plugins {
    `java-library`
    application
}

val jacksonVersion: String by project

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
}

application {
    mainClass.set("dev.turgon.connector.stripe.fake.FakeStripe")
    applicationName = "turgon-fake-stripe"
}
