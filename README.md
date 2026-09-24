# Turgon (working name: Porter)

Turgon is a universal connector for legacy systems and best-of-breed integrations: a neutral,
customer-side integrator that connects legacy cores, custom software, SaaS, data platforms and AI
agents from inside the customer's own environment, on open standards.

This repository starts with the declarative core that the architecture (v0.2) says Porter builds
itself: the object model, the verifier, the integration compiler, and the trust primitives for
governed write-back. Everything else in the stack (Temporal, Kafka, Camel, OPA, OpenBao, Flux...)
is adopted open source that these pieces configure.

## Quick start

```sh
make test                                                  # go test -race ./...
go run ./cmd/porter validate examples                      # schema-check every spec
go run ./cmd/porter verify  -c examples salesforce-won-deals-to-sap-orders
go run ./cmd/porter verify  -c examples eu-distributor-core
go run ./cmd/porter compile -c examples eu-distributor-core -o runtime-spec.json
go run ./cmd/porter audit verify path/to/audit.jsonl
```

`verify` reports every finding and the one-click level reached (L0 certified, L1 assisted,
L2 guided, L3 engineered). `compile` refuses anything that isn't deployable and otherwise
emits a reproducible runtime spec whose `sha256` digest changes only when its inputs do.

## Layout

| Path | Architecture | What it is |
|---|---|---|
| `api/v1alpha1` | §3, §7, §18, App. A–B | Object model: `ConnectorManifest`, `Recipe`, `Mapping`, `SlotContract`, `Plugin`, `StackBlueprint`, `PolicyPack`, with schema validation |
| `pkg/spec` | §2 "declarative everything" | Strict YAML/JSON loader (unknown fields are errors) |
| `pkg/catalog` | §7.1 versioning | Index with semver resolution of refs like `sf-opportunity-to-order@3` |
| `pkg/verifier` | §7.5, §18.1 | Verifier stages: schema, resolve, permitted interfaces, slot contracts, mappings/review queue, policy, capacity; computes L0–L3 |
| `pkg/compiler` | §7.5, AD-04 | Recipe/blueprint → `RuntimeSpec` (connectors, topics, workflows, plugins with grants, MCP tool descriptors, policies, monitors) |
| `pkg/writeguard` | §8 | Idempotency, policy, validation, simulation-first, approval gates, rate governor, circuit breaker, read-your-writes, metering, sagas with compensation |
| `pkg/policy` | §9, App. C | Policy decision interface and the built-in write-back default |
| `pkg/audit` | §9 | Append-only, hash-chained audit log with tamper detection |
| `pkg/semver` | | Version constraints (`^`, `~`, partial, `>=`) |
| `cmd/porter` | §12 CLI | `validate`, `verify`, `compile`, `audit verify` |
| `wit/porter-stack.wit` | §18.4 | Host interface for Wasm plugins |
| `examples/` | App. A–C, §18.5 | SAP ECC, Salesforce, Shopify, Stripe, Power BI connectors; slot contracts; the `eu-distributor-core` blueprint |

## Design notes

- **Sanctioned interfaces only.** A manifest declares permitted and prohibited interfaces; a
  recipe that reaches a prohibited path (e.g. SAP ODP-RFC) is blocked at L3 with the reason.
- **Operations, entities and events on manifests.** Appendix A doesn't list them, but the
  verifier needs them to check recipes and slot contracts, so manifests declare named
  operations with a direction, interface, risk tier and compensation.
- **Slots over tools.** Recipes may name a slot (`erp`) instead of a tool (`sap-ecc`). In a
  blueprint, agent tools are generated from slot contracts, so `create_sales_order` keeps its
  name and meaning when the ERP behind it is swapped; the swap is re-verified against the contract.
- **Every write has an undo.** Write steps without a compensation are rejected; high-risk writes
  cannot disable approval.
- **Deterministic output.** The compiler sorts everything and hashes canonical JSON, so runtime
  specs can live in Git and be diffed, signed and rolled back.

## Not built yet

In rough roadmap order (§16, §19): runtime workers that interpret `RuntimeSpec` on Temporal;
Postgres-backed idempotency store, transactional outbox and metadata graph; real connectors
(Postgres, Salesforce first); JSONata evaluation and sample dry-runs; OPA evaluation of
`PolicyPack`s; MCP server generation behind agentgateway; the console and review queue;
packaging (Helm, operator, Flux); the Wasm plugin host.
