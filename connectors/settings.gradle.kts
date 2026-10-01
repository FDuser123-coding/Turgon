// Turgon's Camel/Java connectors: the SDK that serves the connector
// protocol (proto/turgon/connector/v1) to a Turgon worker, and the
// connectors built on it. Each runs as a sidecar next to the worker.
rootProject.name = "turgon-connectors"

include("sdk", "sap-ecc", "sap-ecc-fake")
