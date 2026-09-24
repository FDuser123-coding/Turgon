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

## Run the prototype flow

`shop-orders-to-erp` is the prototype's end-to-end flow (§19.1): an order written to a web
shop's transactional outbox becomes a sales order in an ERP database. It's mapped with
JSONata, the customer is resolved to a master record, the insert is dry-run inside a
rolled-back transaction, and the order is committed only after a person approves it.
Needs Postgres and a Temporal server (`temporal server start-dev`).

```sh
export PORTER_DATABASE_URL=postgres://localhost:5432/porter_demo
psql "$PORTER_DATABASE_URL" -f examples/sql/demo.sql           # demo shop + ERP schemas
make spec                                                      # -> runtime-spec.json
bin/porter secrets runtime-spec.json                           # env vars for each secretRef
export PORTER_SECRET_SHOP_DB_DSN=$PORTER_DATABASE_URL PORTER_SECRET_ERP_DB_DSN=$PORTER_DATABASE_URL
bin/porter xref set --entity Customer --system shop-db --source ada@example.com --master C-100
bin/porter run -s runtime-spec.json &                          # worker + event dispatcher

psql "$PORTER_DATABASE_URL" -c "insert into shop.outbox (event, payload) values ('Order.Created',
  '{\"order_number\": 2001, \"created_at\": \"2026-09-24T11:00:00Z\", \"total\": \"349.90\",
    \"currency\": \"eur\", \"customer\": {\"email\": \"ada@example.com\"},
    \"items\": [{\"sku\": \"M-7\", \"qty\": 1}]}')"
bin/porter pending shop-orders-to-erp/1                        # the write and its dry-run preview
bin/porter approve shop-orders-to-erp/1 --by you@example.com   # or --reject
bin/porter retry   shop-orders-to-erp/1                        # re-run a failed run (optionally --spec)
bin/porter audit verify porter-audit.jsonl
```

### Salesforce to ERP

`salesforce-won-deals-to-erp` is a saga across two systems: a won opportunity becomes an ERP
sales order, then the ERP order number is written back to the opportunity. If the write-back
fails (say, a validation rule rejects it), the ERP order is cancelled. The connection
authenticates as an integration user with the OAuth 2.0 JWT bearer flow. Without an org, run
the test fake, which checks JWT signatures and pages SOQL results like the real API:

```sh
go build -o bin/fakesf ./internal/tools/fakesf
touch sf.cmds && (tail -f sf.cmds | bin/fakesf > sf.creds &)   # prints the credentials JSON
export PORTER_SECRET_SALESFORCE_PROD_JWT="$(cat sf.creds)"
bin/porter compile -c examples salesforce-won-deals-to-erp -o sf.json
bin/porter xref set --entity Customer --system salesforce-prod --source 001000000000001AAA --master C-100
bin/porter run -s sf.json --audit-log sf-audit.jsonl &
echo "001000000000001AAA 7800" >> sf.cmds                      # a deal is won
bin/porter approve salesforce-won-deals-to-erp/006000000000001AAA --by you@example.com
```

Each spec runs on its own Temporal task queue (`porter-<spec name>`), so workers for different
specs can share a cluster.

### Console

`porter console` serves the web console: the approval queue (each pending write with its
dry-run preview, the reasons policy asked for a person, and approve/reject with a note that
goes into the audit log), runs with their writes and failures, the audit logs with live chain
verification, and verifier reports for the catalog including the mapping review queue.

```sh
make console build                  # builds the React app and embeds it in bin/porter
bin/porter console -c examples --audit-log porter-audit.jsonl --dev-user you@example.com
# open http://127.0.0.1:8080
```

`--auth dev` treats every request as `--dev-user` and only listens on loopback. In production
use `--auth proxy` behind an authenticating reverse proxy such as oauth2-proxy with the
customer's identity provider: it trusts `X-Auth-Request-Email` and `X-Auth-Request-Groups`
only from `--trusted-proxy` addresses, and only members of `--approver-group` may decide.
A decision always refers to the exact request shown (by its SHA-256 digest), nobody can
approve a write made on their own behalf, and cross-site requests are refused.
For UI development, `cd console && npm run dev` proxies `/api` to a running console.

### Install on Kubernetes

The chart in `deploy/helm/porter` installs one worker per compiled spec and the console into
a single namespace: no cluster roles, no service-account tokens, the "restricted" pod security
profile, read-only root filesystems and deny-by-default network policies. Porter's state and
the shared, hash-chained audit log live in Postgres: an existing database, or a CloudNativePG
cluster the chart creates. Temporal is expected to be reachable at `temporal.address`.

```sh
kubectl preflight deploy/preflight.yaml                 # Troubleshoot: version, sizing, storage
bin/porter compile -c examples shop-orders-to-erp -o shop.json
kubectl -n integrations create secret generic porter-connection-secrets \
  --from-literal=PORTER_SECRET_SHOP_DB_DSN=... --from-literal=PORTER_SECRET_ERP_DB_DSN=...
helm install porter deploy/helm/porter -n integrations \
  --set database.cloudNativePG.enabled=true \
  --set console.auth.trustedProxies={10.42.0.0/16} --set console.auth.approverGroup=porter-approvers \
  --set 'workers.secretEnvFrom={porter-connection-secrets}' \
  --set-file specs.shop-orders-to-erp=shop.json
helm test porter -n integrations                        # runs `porter check` for every spec
```

`porter check -s spec.json` is the pipeline's Connect stage: it tests each connection (reachability,
tables, columns, unique keys, privileges; Salesforce login, objects and field access) and prints a
plain-language fix for each failure. `deploy/flux/porter.yaml` shows pull-based delivery with Flux,
cosign verification and automatic rollback. The image is built by the `Dockerfile` (distroless,
non-root, static binary).

### Console demo on Vercel

Vercel builds only the console, in demo mode with sample data (`vercel.json`, `.vercelignore`):
the real API runs inside the customer's environment and is never exposed publicly. The demo
build is labelled as such, keeps decisions in the browser tab, and its sample data is not part
of the console embedded in `porter`.

Run IDs are `<workflow>/<event id>`, so an event starts at most one successful run. A failed
run can be retried; writes it already committed are recognized by their idempotency keys.
Integration tests use a real Postgres when `PORTER_TEST_DATABASE_URL` is set
(`make test-integration`); workflow tests use Temporal's in-process test server.

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
| `pkg/mapping` | §7.4 | JSONata evaluation of mapping sets |
| `pkg/engine` | §7.6, §8, AD-04/06 | One generic Temporal workflow that interprets any compiled workflow; activities for map, resolve, two-phase governed writes and compensation; durable approval signal; event dispatcher |
| `pkg/connector` | §7.1 | Runtime connector interfaces, registry, secret resolution; `postgres/` is the native Postgres connector (outbox events, rollback dry-runs, idempotent writes) |
| `pkg/connector/salesforce` | §7.1, §13 | Native Salesforce connector: OAuth JWT bearer or client credentials, SOQL polling on `SystemModstamp`, updates that record previous values, restore for compensation; `sftest/` is a fake org for tests |
| `pkg/store/pgstore` | §7.2, §7.3, §8 | Porter's state in Postgres: idempotency records with leases, source cursors, identity cross-references |
| `pkg/semver` | | Version constraints (`^`, `~`, partial, `>=`) |
| `pkg/console` | §12 | Console API (runs, approvals, audit, catalog), proxy/dev authentication, embedded web app |
| `console/` | §12 | The web console: React + TypeScript, built with Vite |
| `deploy/` | §11 | Helm chart, Flux example, Troubleshoot preflight spec |
| `cmd/porter` | §12 CLI | `validate`, `verify`, `compile`, `audit verify`, `run`, `pending`, `approve`, `retry`, `xref set`, `secrets`, `console`, `check` |
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
- **Connections.** A `Connection` binds a connector to one system (pipeline stage 1). Generic
  connectors like Postgres get their entities, events and operations from it, but only
  through interfaces the connector's manifest permits.
- **Approvals bind to content.** A pending approval carries the SHA-256 of the write request;
  a decision must echo it and name the waiting step, so an early, stale or misdirected
  decision can never approve a write nobody looked at.
- **Two-phase writes.** The write guard's `Prepare` (policy, validation, dry-run) and
  `Commit` phases let a workflow wait durably for a person between them.
- **Write outputs.** A write step's `output` puts its result into the document, so later steps
  can use it (the ERP order number linked back to Salesforce).
- **Recipe conventions (prototype).** Resolve steps read `<entity>Ref` and set `<entity>Id`
  (`customerRef` → `customerId`); approval thresholds read the amount from `netValue`.
- **Deterministic output.** The compiler sorts everything and hashes canonical JSON, so runtime
  specs can live in Git and be diffed, signed and rolled back.

## Not built yet

In rough roadmap order (§16, §19): Salesforce Pub/Sub API change capture and Bulk API reads;
the Porter operator and signed releases (cosign, SBOMs); an appliance build (§11);
Debezium change capture in place of outbox polling; probabilistic identity
resolution (Splink) and the data-steward queue; OPA evaluation of `PolicyPack`s; the metadata graph and discovery; MCP server
generation behind agentgateway; direct OIDC sign-in for the console and approving mapping
fields from its review queue; packaging (Helm, operator,
Flux); the Wasm plugin host. The native Postgres and Salesforce connectors run inside the Go
worker for the prototype; production connectors run on the Camel/Java worker types in §7.1.
