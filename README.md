# Turgon

Turgon is a universal connector for legacy systems and best-of-breed integrations: a neutral,
customer-side integrator that connects legacy cores, custom software, SaaS, data platforms and AI
agents from inside the customer's own environment, on open standards.

This repository starts with the declarative core that the architecture (v0.2) says Turgon builds
itself: the object model, the verifier, the integration compiler, and the trust primitives for
governed write-back. Everything else in the stack (Temporal, Kafka, Camel, OPA, OpenBao, Flux...)
is adopted open source that these pieces configure.

## Quick start

```sh
make test                                                  # go test -race ./...
go run ./cmd/turgon validate examples                      # schema-check every spec
go run ./cmd/turgon verify  -c examples salesforce-won-deals-to-sap-orders
go run ./cmd/turgon verify  -c examples eu-distributor-core
go run ./cmd/turgon compile -c examples eu-distributor-core -o runtime-spec.json
go run ./cmd/turgon audit verify path/to/audit.jsonl
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
export TURGON_DATABASE_URL=postgres://localhost:5432/turgon_demo
psql "$TURGON_DATABASE_URL" -f examples/sql/demo.sql           # demo shop + ERP schemas
make spec                                                      # -> runtime-spec.json
bin/turgon secrets runtime-spec.json                           # env vars for each secretRef
export TURGON_SECRET_SHOP_DB_DSN=$TURGON_DATABASE_URL TURGON_SECRET_ERP_DB_DSN=$TURGON_DATABASE_URL
bin/turgon xref set --entity Customer --system shop-db --source ada@example.com --master C-100
bin/turgon run -s runtime-spec.json &                          # worker + event dispatcher

psql "$TURGON_DATABASE_URL" -c "insert into shop.outbox (event, payload) values ('Order.Created',
  '{\"order_number\": 2001, \"created_at\": \"2026-09-24T11:00:00Z\", \"total\": \"349.90\",
    \"currency\": \"eur\", \"customer\": {\"email\": \"ada@example.com\"},
    \"items\": [{\"sku\": \"M-7\", \"qty\": 1}]}')"
bin/turgon pending shop-orders-to-erp/1                        # the write and its dry-run preview
bin/turgon approve shop-orders-to-erp/1 --by you@example.com   # or --reject
bin/turgon retry   shop-orders-to-erp/1                        # re-run a failed run (optionally --spec)
bin/turgon audit verify turgon-audit.jsonl
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
export TURGON_SECRET_SALESFORCE_PROD_JWT="$(cat sf.creds)"
bin/turgon compile -c examples salesforce-won-deals-to-erp -o sf.json
bin/turgon xref set --entity Customer --system salesforce-prod --source 001000000000001AAA --master C-100
bin/turgon run -s sf.json --audit-log sf-audit.jsonl &
echo "001000000000001AAA 7800" >> sf.cmds                      # a deal is won
bin/turgon approve salesforce-won-deals-to-erp/006000000000001AAA --by you@example.com
```

### Any HTTP API: Shopify to ERP

The `rest` connector integrates HTTP JSON APIs (Shopify, Stripe, HubSpot, in-house services) with
configuration only: each Connection declares its events, reads and writes as HTTP requests
(`examples/connections/shopify-store.yaml`).

- **Events** are list requests polled with a cursor the API understands (`since_id`,
  `created[gt]`, `updated_at_min`), or search requests with a JSON body (HubSpot's
  `POST .../search`); timestamp cursors never split a group of equal timestamps. An event can
  also arrive by **signed webhook** (see below).
  Newest-first lists (Stripe) are paged back to the cursor with `starting_after` / `has_more`,
  so a burst of new items is read oldest first across polls and never skipped.
- **Reads** fetch one record by ID and return business field names; they become agent tools.
- **Writes** are one request each: a method, a path templated from the payload (values escaped as
  path segments), and a body built from mapped fields, optionally wrapped (`{"order": {...}}`),
  as JSON or form-encoded (`metadata[erp_payment_id]=42`, as Stripe takes it).
  Where the API takes one, the idempotency key is sent as a header (`Idempotency-Key`).
- **Updates can capture** the fields they change first. That previews the change for the approver
  (current and proposed values), confirms it by reading it back, and lets a `restore` operation
  undo it exactly in a saga (`nullValue: ""` clears a field for APIs such as HubSpot that do
  not take null). A create is undone by a request templated from its own result
  (`DELETE /invoices/{{id}}`).
- **Auth**: a bearer token, an API-key header, basic, or OAuth 2.0 client credentials (refreshed
  before expiry and after a 401), always from the connection's secret.
- Client errors fail the step at once; rate limits and server errors are retried.
- **Limits**: each connection declares its API's own rate and concurrency limits
  (`spec.limits`), which the verifier checks the recipe's peak load against and the rate governor
  enforces. A connector with its own operations (such as SAP) can only have them lowered.

`shopify-store-orders-to-erp` is a saga: each new Shopify order becomes an ERP sales order (a
person approves it), then the ERP order number is written into the Shopify order's note. If
Shopify rejects the note, the ERP order is cancelled. Without a store, run the test fake:

```sh
go build -o bin/fakeshop ./internal/tools/fakeshop
touch shop.cmds && (tail -f shop.cmds | bin/fakeshop &)        # http://127.0.0.1:9300, token shpat_demo
cp -r examples my-catalog && sed -i 's|https://turgon-demo.myshopify.com|http://127.0.0.1:9300|' \
  my-catalog/connections/shopify-store.yaml
bin/turgon compile -c my-catalog shopify-store-orders-to-erp -o shopify.json
export TURGON_SECRET_SHOPIFY_STORE_TOKEN=shpat_demo
bin/turgon check -s shopify.json                                # credential, events, ERP tables
bin/turgon xref set --entity Customer --system shopify-store --source ada@example.com --master C-100
bin/turgon run -s shopify.json &
echo "ada@example.com 310.00" >> shop.cmds                      # an order is placed
```

Approve it in the console (or with `turgon approve`); the fake prints the order's new note.

### Stripe to ERP: cash application

`stripe-payments-to-erp` records every paid Stripe invoice as an ERP payment and writes the ERP
payment ID into the invoice's metadata; if Stripe rejects that, the ERP payment is voided.
Payments are low risk, so they flow without a person, except amounts above the approval
threshold (50,000 by default), which wait for approval like any other write.

```sh
go build -o bin/fakestripe ./internal/tools/fakestripe
touch stripe.cmds && (tail -f stripe.cmds | bin/fakestripe &)  # http://127.0.0.1:9400, key rk_test_demo
cp -r examples my-catalog && sed -i 's|https://api.stripe.com|http://127.0.0.1:9400|' \
  my-catalog/connections/stripe-billing.yaml
bin/turgon compile -c my-catalog stripe-payments-to-erp -o stripe.json
export TURGON_SECRET_STRIPE_BILLING_RESTRICTED_KEY=rk_test_demo
bin/turgon check -s stripe.json
bin/turgon xref set --entity Customer --system stripe-billing --source cus_ada --master C-100
bin/turgon run -s stripe.json &
echo "cus_ada 49.90" >> stripe.cmds                              # recorded at once
echo "cus_ada 75000" >> stripe.cmds                              # waits for approval
```

### HubSpot to ERP: order entry from the CRM

`hubspot-won-deals-to-erp` turns every deal won in HubSpot into an ERP sales order (a person
approves it) and records the ERP order number on the deal. Won deals are found with the search
API: deals in the closed-won stage without an ERP order number, in order of their last change
(`examples/connections/hubspot-crm.yaml`). Writing the number takes the deal out of that
search, so the write-back never starts a second run. If HubSpot rejects the update, the ERP
order is cancelled. The buyer is matched by email and company domain against customers known
from other systems; a new buyer at a known company goes to a data steward with the company
suggested.

```sh
go build -o bin/fakehubspot ./internal/tools/fakehubspot
touch hs.cmds && (tail -f hs.cmds | bin/fakehubspot &)          # http://127.0.0.1:9500, token pat-demo
cp -r examples my-catalog && sed -i 's|https://api.hubapi.com|http://127.0.0.1:9500|' \
  my-catalog/connections/hubspot-crm.yaml
bin/turgon compile -c my-catalog hubspot-won-deals-to-erp -o hubspot.json
export TURGON_SECRET_HUBSPOT_CRM_PRIVATE_APP_TOKEN=pat-demo
bin/turgon check -s hubspot.json
bin/turgon xref set --entity Customer --system shop-db --source ada@lovelace-gmbh.example \
  --email ada@lovelace-gmbh.example --master C-100
bin/turgon run -s hubspot.json &
echo "won ada@lovelace-gmbh.example 1500" >> hs.cmds              # matched, waits for approval
echo "won grace@lovelace-gmbh.example 900" >> hs.cmds             # goes to the steward queue
```

The deal needs two custom properties: `customer_email` (a HubSpot workflow can copy it from the
deal's primary contact) and `erp_order_number`.

### Webhooks: events in seconds, polling as the safety net

Turgon runs next to the customer's systems, often where nothing may connect in, so events are
polled by default. Where the provider can reach a worker, an event can also arrive by webhook:
the connection's event gets a `webhook` block (`examples/connections/stripe-billing.yaml`,
`shopify-store.yaml`) and workers run with `--webhook-listen`. Then:

- **Deliveries are verified** before anything is stored: Stripe's `Stripe-Signature`
  (timestamped, so old deliveries cannot be replayed), Shopify's `X-Shopify-Hmac-Sha256`, or
  any HMAC-SHA256 of the body in a header (hex or base64, with a prefix such as `sha256=`).
  The signing secret is its own secret reference (`turgon secrets` lists it). Forged or
  expired deliveries get 401; bodies are capped at 4 MiB.
- **A delivery is stored before it is acknowledged**, in an inbox table in Turgon's database,
  so an acknowledged event is never lost; if storing fails the provider is told to retry. The
  dispatcher is woken and the run starts at once (6 ms after the delivery, in the demo below).
- **Polling reconciles.** Every `--reconcile` (5 minutes by default) the event is also polled,
  into the same inbox, to catch deliveries the provider gave up on. The inbox keeps each
  event ID once, so an event that arrives both ways, or is redelivered, starts one run, and a
  run that ended (an approval that was rejected) is never started again.
- A worker without `--webhook-listen` polls as before and needs no signing secret.

```sh
bin/turgon compile -c my-catalog stripe-payments-to-erp -o stripe.json   # as above
export TURGON_SECRET_STRIPE_BILLING_WEBHOOK_SECRET=whsec_demo
bin/turgon run -s stripe.json --webhook-listen 127.0.0.1:8082 --reconcile 30s &
touch stripe.cmds && (tail -f stripe.cmds | bin/fakestripe -webhook-url \
  http://127.0.0.1:8082/webhooks/stripe-billing/Invoice.Paid -webhook-secret whsec_demo &)
echo "cus_ada 49.90" >> stripe.cmds            # delivered: the run starts at once
echo "cus_ada 12.00 nohook" >> stripe.cmds     # never delivered: the next reconcile finds it
```

In Kubernetes, `workers.webhooks.enabled` opens the port on each worker behind a Service, and
`workers.webhooks.ingress` routes exactly the webhook paths of each spec's webhook events.

Each spec runs on its own Temporal task queue (`turgon-<spec name>`), so workers for different
specs can share a cluster.

### Performance

`scripts/loadtest.sh [payments]` measures Stripe-to-ERP cash application end to end: signed
webhooks from the fake Stripe, the worker, the write guard with previews and read-back
confirmation, the ERP in Postgres and the write-back to Stripe. On a 4-core machine with the
Temporal dev server in memory, 1,000 payments arriving at once:

| | Throughput | Run latency p50 / p95 / p99 |
|---|---|---|
| `--pollers auto` (default) | 36.5 payments/s | 0.86 s / 1.54 s / 1.66 s |
| `--pollers 2` (the Temporal SDK's default) | 31.6 payments/s | 9.9 s / 15.5 s / 16.5 s |

At that rate the Temporal server uses three of the four cores and the worker less than one:
Temporal is the limit, and it scales on its own cluster. Map and resolve steps run as local
activities, in the worker and within the workflow's own task, so a run costs Temporal 5
workflow tasks and 4 activity tasks. A connection's declared rate limit applies before any of
this: each payment makes several Stripe requests (the preview, the update and its read-back),
so the example connection's 20 requests a second, not Turgon, sets the pace against real
Stripe; the load test lifts that limit to measure Turgon. The dev server's on-disk SQLite, by contrast, allows only a few
runs a second, so load tests run it in memory.

### Notifications

Workers tell people when a run needs them, so nobody has to watch the console:

| Event | Who acts | Link |
|---|---|---|
| A write waits for approval (a recipe's, or one an agent asked for: the message names the agent and the user it acts for) | an approver, before the deadline shown | the run |
| Nobody decided in time (the write was rejected) | an operator, who retries the run to ask again | the run |
| A record matches no master record | a data steward | the steward queue |
| A step failed and its earlier writes could not be undone | an operator, to reconcile | the run |

Channels are set with environment variables, because webhook URLs are credentials:
`TURGON_NOTIFY_SLACK_WEBHOOK_URL` (a Slack incoming webhook, Block Kit message with an "Open
in Turgon" button), `TURGON_NOTIFY_TEAMS_WEBHOOK_URL` (a Teams workflow webhook, Adaptive
Card) and `TURGON_NOTIFY_WEBHOOK_URL` with `TURGON_NOTIFY_WEBHOOK_SECRET` (the notification
as JSON, signed `Turgon-Signature: t=…,v1=<HMAC-SHA256 of "t.body">`). `--console-url` (or
`TURGON_CONSOLE_URL`, `workers.consoleURL` in the chart) makes the links.

- Messages say what waits and where to act, never payload values: customer data stays in the
  console, behind its sign-in. Values that do appear are escaped, so a source record cannot
  inject Slack mentions or links.
- Notifying is best effort: a channel that keeps failing is retried a few times and then
  skipped, and the run goes on. A chat outage never holds up a write.
- Runs that were already waiting when workers were upgraded carry on without notifying
  (a workflow version gate); `pkg/engine/testdata/histories` holds histories recorded from
  real runs before and after the change, which the tests replay against the current code.

### Console

`turgon console` serves the web console: the approval queue (each pending write with its
dry-run preview, the reasons policy asked for a person, and approve/reject with a note that
goes into the audit log), the data-steward queue, runs with their writes and failures, the audit logs with live chain
verification, and verifier reports for the catalog including the mapping review queue.

```sh
make console build                  # builds the React app and embeds it in bin/turgon
bin/turgon console -c examples --audit-log turgon-audit.jsonl --dev-user you@example.com
# open http://127.0.0.1:8080
```

`--auth dev` treats every request as `--dev-user` and only listens on loopback. In production
the console signs people in with the customer's identity provider, in one of two ways:

- **`--auth oidc`**: the console is an OpenID Connect client itself (Microsoft Entra ID, Okta,
  Keycloak, Google...). It uses the authorization code flow with PKCE, checks the ID token's
  signature, issuer, audience, expiry and nonce, and keeps the user in an HttpOnly,
  SameSite=Lax session cookie signed with `TURGON_CONSOLE_SESSION_KEY` (the same on every
  replica); the client secret is read from `TURGON_OIDC_CLIENT_SECRET`. Roles come from the
  groups claim (`--oidc-groups-claim`) at sign-in and last for `--session-ttl` (8 hours).
  Register `<--url>/auth/callback` as the app's redirect URI. A page opened without a session,
  such as a link from a notification, goes through sign-in and comes back to that page.
- **`--auth proxy`**: behind an authenticating reverse proxy such as oauth2-proxy, trusting
  `X-Auth-Request-Email` and `X-Auth-Request-Groups` only from `--trusted-proxy` addresses.

Either way, only members of `--approver-group` may decide.
A decision always refers to the exact request shown (by its SHA-256 digest), nobody can
approve a write made on their own behalf, and cross-site requests are refused.
For UI development, `cd console && npm run dev` proxies `/api` to a running console.

**Retrying failed runs.** A failed run's page offers **Retry run** to operators
(`--operator-group`): after an approval nobody answered in time, or once the cause of a failure
is fixed. The reason is required and recorded as `run.retried` in the audit log before the run
starts again from its event; writes it already made are not repeated, because it reuses their
idempotency keys. Only failed, timed-out, terminated or cancelled runs can be retried.

**Data-steward queue.** A run stops when a source record has no master record (a new customer's
email, a Stripe customer ID), rather than guess. The console's Steward page lists each missing
link once, with the runs waiting on it and the source record. A steward (`--steward-group`)
enters the master record's ID: the console stores the cross-reference, records `xref.linked`
under the steward's name in the audit log, and starts every waiting run again (`run.retried`).
Only links the queue is waiting for can be made, so the page cannot rewrite other references.
The queue needs Turgon's state database (`--database-url` or `TURGON_DATABASE_URL`);
`turgon xref set` does the same from the command line.

**Identity matching** (§7.3). A resolve step names the fields that identify a record and how to
compare them: `email` (the same address), `domain` (the same company email domain; free mail
providers say nothing), `name` (similar names, Jaro-Winkler) or `exact` (e.g. a VAT ID). Every
cross-reference keeps its record's attributes, and a new record is scored against the records
already linked with a Fellegi-Sunter model, as record-linkage tools such as Splink do:

```yaml
- resolve:
    entity: model.Customer
    strategy: probabilistic      # or exact: never links on its own, but stewards get suggestions
    autoMatchAbove: 0.95
    match:
      - { field: customerRef, kind: email }
      - { field: customerRef, kind: domain }
```

With `probabilistic`, a match at or above `autoMatchAbove` whose runner-up is below 0.5 is linked
and audited as `xref.matched` (score and reasons); anything else goes to the steward queue with
the best suggestions, such as "C-100, 88%, same company email domain", one click to use. Each
link a steward confirms keeps the record's attributes, so the next order from that buyer links
at once. `turgon xref set --email --name` seeds known contacts. The model's weights are
conservative defaults for customer data, not trained per deployment; an external Splink
service (`strategy: splink`) is not wired in, and records resolved that way go to the steward.

### Tools for AI agents (MCP)

`turgon mcp` serves a spec's read-only tools to AI agents over MCP (streamable HTTP). Tools are
generated from the connections' read operations and named in business terms, such as
`get_customer` or `get_sales_order`; when two systems offer the same operation, both are
qualified (`erp_db_get_customer`, `salesforce_prod_get_customer`). A Salesforce read returns
business field names (`name`, `city`) rather than `BillingCity`.

```sh
bin/turgon mcp -s runtime-spec.json --dev-agent claude --dev-user you@example.com   # loopback only
bin/turgon mcp -s runtime-spec.json --auth gateway --trusted-gateway 10.42.0.0/16    # behind agentgateway
```

Every call is authorized for the agent and the person it acts for (the built-in policy lets
`integration-reader` and `integration-operator` read), passes the system's rate limit and circuit
breaker, and is audited as "agent for user", without the data returned. Agents never see system
credentials. Behind the gateway, identity comes from `X-Agent-Id`, `X-On-Behalf-Of` and
`X-Agent-Roles`, trusted only from `--trusted-gateway` addresses.

With `--writes`, the recipe's write operations are served too (`create_sales_order`), taking a
record whose fields come from the mapping that feeds the write. A call starts a governed write
on Temporal (architecture §8, figure 4), run by the `turgon run` workers of the same spec:
policy, validation and the target's dry-run first; then, for high-risk tools or large amounts, a
person approves the previewed write in the console; then the commit, audited as the agent for
its user. Agents need the `integration-operator` role to ask for a write at all.

```sh
bin/turgon run -s runtime-spec.json                                   # workers commit the writes
bin/turgon mcp -s runtime-spec.json --writes --dev-agent claude --dev-user you@example.com --dev-roles integration-operator
```

Each write carries the agent's own `requestId`. A call waits up to `--wait` (15s) and answers
`committed`, `pending_approval` (with the preview), or why nothing was written; calling again
with the same arguments reports how the write stands and never writes twice, and reusing a
`requestId` for a different record is refused. Unanswered approvals are rejected after
`--approval-timeout` (72h).

#### Turgon as an A2A agent

The same server is an [A2A](https://a2a-protocol.org) agent (protocol 0.3, JSON-RPC), so other
agents can delegate integration tasks to Turgon (§7.7). Its agent card is at
`/a2a/.well-known/agent-card.json` and lists the tools as skills. Requests are structured, not
prose, because Turgon runs no language model at run time: a message carries a data part
`{"skill": "create_sales_order", "arguments": {...}}` with the MCP tool's arguments.

- A read answers at once with a message holding the record.
- A write becomes a task whose ID is its durable agent-write workflow, so it can wait days for
  approval and survive restarts: `working` while it waits, then `completed` with the result as
  an artifact, or `rejected` / `failed` with the reason. Poll it with `tasks/get`; only the agent
  that asked can see it, and a person, not the agent, decides it (`tasks/cancel` is refused).
- A write's `requestId` defaults to the message ID, so resending a message never writes twice.

Streaming and push notifications are not offered. Behind agentgateway each spec's agent is at
`/<spec>/a2a`, with the same token authentication and identity headers; the gateway rewrites
the card's URL to its own.

### Install on Kubernetes

The chart in `deploy/helm/turgon` installs one worker per compiled spec and the console into
a single namespace: no cluster roles, no service-account tokens, the "restricted" pod security
profile, read-only root filesystems and deny-by-default network policies. Turgon's state and
the shared, hash-chained audit log live in Postgres: an existing database, or a CloudNativePG
cluster the chart creates. Temporal is expected to be reachable at `temporal.address`.

```sh
kubectl preflight deploy/preflight.yaml                 # Troubleshoot: version, sizing, storage
bin/turgon compile -c examples shop-orders-to-erp -o shop.json
kubectl -n integrations create secret generic turgon-connection-secrets \
  --from-literal=TURGON_SECRET_SHOP_DB_DSN=... --from-literal=TURGON_SECRET_ERP_DB_DSN=...
helm install turgon deploy/helm/turgon -n integrations \
  --set database.cloudNativePG.enabled=true \
  --set console.auth.trustedProxies={10.42.0.0/16} --set console.auth.approverGroup=turgon-approvers \
  --set 'workers.secretEnvFrom={turgon-connection-secrets}' \
  --set-file specs.shop-orders-to-erp=shop.json
helm test turgon -n integrations                        # runs `turgon check` for every spec
```

To have the console sign people in itself instead of running oauth2-proxy, create a Secret
with `TURGON_OIDC_CLIENT_SECRET` and `TURGON_CONSOLE_SESSION_KEY` and set
`console.auth.mode=oidc` with `console.auth.oidc.{issuer,clientID,url,existingSecret}`.

To receive webhooks, add the signing secrets to the connection secrets and
`--set workers.webhooks.enabled=true --set workers.webhooks.ingress.enabled=true
--set workers.webhooks.ingress.host=hooks.example.com`; the ingress routes only
`/webhooks/<endpoint>/<event>` for the events configured for webhooks.

#### Agents behind agentgateway

With `agents.enabled`, the chart runs [agentgateway](https://agentgateway.dev) (v1.5.0) in front
of one `turgon mcp` per spec, as the architecture's agent gateway (§7.7). The MCP servers run in
the gateway's pod and listen on loopback only, so the gateway is the only way in and the only
party that can set the identity headers they trust.

```sh
helm upgrade turgon deploy/helm/turgon -n integrations --reuse-values \
  --set agents.enabled=true --set 'agents.writes={shop-orders-to-erp}' \
  --set agents.gateway.issuer=https://login.example.com \
  --set agents.gateway.jwksURL=https://login.example.com/.well-known/jwks.json \
  --set 'agents.gateway.audiences={turgon}' --set agents.gateway.rateLimit=100/1m
```

Agents connect to `/<spec>/mcp` with a bearer token from your identity provider. The gateway
requires a valid token and drops it before forwarding; it passes the agent (`azp` claim), the
user it acts for (`sub`) and the roles (`roles`, a list) as headers, and when a claim is missing
it removes the header rather than forwarding one a caller sent. It lists and allows tools by
risk tier: read tools for `integration-reader` or `integration-operator`, write tools for
`integration-operator`, so agents never see tools they cannot use. Turgon's own policy still
decides, and audits, every call. The claims, audiences and a per-spec rate limit are
configurable. The configuration is generated from the specs at start by
`turgon gateway-config`, which you can also run yourself; `helm test` has agentgateway
validate it.

`turgon check -s spec.json` is the pipeline's Connect stage: it tests each connection (reachability,
tables, columns, unique keys, privileges; Salesforce login, objects and field access) and prints a
plain-language fix for each failure. `deploy/flux/turgon.yaml` shows pull-based delivery with Flux,
cosign verification and automatic rollback. The image is built by the `Dockerfile` (distroless,
non-root, static binary).

### Releases and verifying them

Pushing a tag such as `v0.2.0` runs `.github/workflows/release.yml`. It tests, builds a multi-arch
image with build provenance, blocks on critical vulnerabilities (Trivy), and publishes:

- `ghcr.io/fduser123-coding/turgon:<version>`, signed with cosign and carrying a signed SPDX SBOM;
- the Helm chart at `oci://ghcr.io/fduser123-coding/charts/turgon`, signed;
- a GitHub release with static binaries, `SHA256SUMS` and its Sigstore bundle, and a source SBOM.

Signing is keyless (Sigstore with GitHub's OIDC token), so there is no signing key to protect;
the signature names the workflow that made it. To verify:

```sh
ID='^https://github\.com/FDuser123-coding/Turgon/\.github/workflows/release\.yml@refs/tags/v'
ISSUER=https://token.actions.githubusercontent.com
cosign verify ghcr.io/fduser123-coding/turgon:0.2.0 --certificate-identity-regexp "$ID" --certificate-oidc-issuer $ISSUER
cosign verify-attestation --type spdxjson ghcr.io/fduser123-coding/turgon:0.2.0 --certificate-identity-regexp "$ID" --certificate-oidc-issuer $ISSUER
cosign verify-blob --bundle SHA256SUMS.sigstore.json --certificate-identity-regexp "$ID" --certificate-oidc-issuer $ISSUER SHA256SUMS
```

In the cluster, `deploy/kyverno/verify-turgon-images.yaml` admits Turgon pods only with an image
signed by that workflow and a signed SBOM, and pins the verified digest; the Flux example
checks the chart's signature against the same identity. CI checks every shipped Go and npm
dependency against the license policy (`scripts/check-licenses.sh`: Apache-2.0, MIT, BSD, ISC
and MPL-2.0 pass; anything else needs a recorded review).

### Console demo on Vercel

Vercel builds only the console, in demo mode with sample data (`vercel.json`, `.vercelignore`):
the real API runs inside the customer's environment and is never exposed publicly. The demo
build is labelled as such, keeps decisions in the browser tab, and its sample data is not part
of the console embedded in `turgon`.

Run IDs are `<workflow>/<event id>`, so an event starts at most one successful run. A failed
run can be retried; writes it already committed are recognized by their idempotency keys.
Integration tests use a real Postgres when `TURGON_TEST_DATABASE_URL` is set
(`make test-integration`); workflow tests use Temporal's in-process test server.

## Layout

| Path | Architecture | What it is |
|---|---|---|
| `apis/v1alpha1` | §3, §7, §18, App. A–B | Object model: `ConnectorManifest`, `Recipe`, `Mapping`, `SlotContract`, `Plugin`, `StackBlueprint`, `PolicyPack`, with schema validation |
| `pkg/spec` | §2 "declarative everything" | Strict YAML/JSON loader (unknown fields are errors) |
| `pkg/catalog` | §7.1 versioning | Index with semver resolution of refs like `sf-opportunity-to-order@3` |
| `pkg/verifier` | §7.5, §18.1 | Verifier stages: schema, resolve, permitted interfaces, slot contracts, mappings/review queue, policy, capacity; computes L0–L3 |
| `pkg/compiler` | §7.5, AD-04 | Recipe/blueprint → `RuntimeSpec` (connectors, topics, workflows, plugins with grants, MCP tool descriptors, policies, monitors) |
| `pkg/writeguard` | §8 | Idempotency, policy, validation, simulation-first, approval gates, rate governor, circuit breaker, read-your-writes, metering, sagas with compensation |
| `pkg/policy` | §9, App. C | Policy decision interface and the built-in write-back default; `opa/` evaluates and tests Rego packs with Open Policy Agent |
| `pkg/audit` | §9 | Append-only, hash-chained audit log with tamper detection |
| `pkg/mapping` | §7.4 | JSONata evaluation of mapping sets |
| `pkg/engine` | §7.6, §8, AD-04/06 | One generic Temporal workflow that interprets any compiled workflow, and one for agent writes; activities for map, resolve, two-phase governed writes and compensation; durable approval signal; event dispatcher; webhook receiver with a durable inbox, reconciled by polling |
| `pkg/connector` | §7.1 | Runtime connector interfaces, registry, secret resolution; `postgres/` is the native Postgres connector (outbox events, rollback dry-runs, idempotent writes) |
| `pkg/connector/rest` | §7.1 | Generic HTTP JSON API connector configured per connection: cursor-polled list or search events (ascending, or newest-first paged back to the cursor), also received as signed webhooks (Stripe, Shopify, generic HMAC), reads, templated JSON or form-encoded writes, captured updates with preview, confirmation and restore; bearer, API-key header, basic and OAuth 2.0 client-credentials auth; `shoptest/`, `stripetest/` and `hubspottest/` fake the Shopify Admin, Stripe and HubSpot CRM APIs |
| `pkg/connector/salesforce` | §7.1, §13 | Native Salesforce connector: OAuth JWT bearer or client credentials, SOQL polling on `SystemModstamp`, updates that record previous values, restore for compensation; `sftest/` is a fake org for tests |
| `pkg/store/pgstore` | §7.2, §7.3, §8 | Turgon's state in Postgres: idempotency records with leases, source cursors, identity cross-references, the webhook inbox |
| `pkg/semver` | | Version constraints (`^`, `~`, partial, `>=`) |
| `pkg/agent` | §7.7, §8 | MCP server and A2A agent: business read tools, and write tools that start approval-gated writes; gateway identity, per-call policy and audit; agentgateway configuration |
| `pkg/notify` | §7.3, §8 | Notifications when a run needs a person: Slack, Microsoft Teams, or a signed JSON webhook, with console links |
| `pkg/identity` | §7.3 | Record matching: normalized identifying attributes, Fellegi-Sunter scoring with Jaro-Winkler names, suggestions and the automatic-match decision |
| `pkg/console` | §7.3, §12 | Console API (runs and retries, approvals, the data-steward queue, audit, catalog), OpenID Connect sign-in or proxy authentication, embedded web app |
| `console/` | §12 | The web console: React + TypeScript, built with Vite |
| `deploy/` | §9, §11 | Helm chart, Flux example, Kyverno signature policy, Troubleshoot preflight spec |
| `cmd/turgon` | §12 CLI | `validate`, `verify`, `compile`, `audit verify`, `run`, `pending`, `approve`, `retry`, `xref set`, `secrets`, `console`, `check`, `mcp`, `gateway-config` |
| `wit/turgon-stack.wit` | §18.4 | Host interface for Wasm plugins |
| `examples/` | App. A–C, §18.5 | SAP ECC, Salesforce, Shopify, Stripe, HubSpot, Power BI connectors and connections; slot contracts; the `eu-distributor-core` blueprint |

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
- **Policy is Rego, evaluated by OPA.** Each recipe's writes are decided by its own policy
  packs (compiled into the runtime spec, so the digest covers them), combined in the package
  `turgon.writeback` through `allow`, `require_approval`, `reasons` and `deny`. A `deny` stops a
  write before anyone is asked to approve it. `turgon verify` compiles the packs and runs their
  `test_` rules; a failing test blocks deployment. Recipes without a `turgon.writeback` pack
  use the built-in default, which a test keeps identical to `examples/policies/writeback-default.yaml`.
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
the Turgon operator; an appliance build (§11);
Debezium change capture in place of outbox polling; training identity-matching weights per deployment, and an external Splink service; signed OPA bundles; the metadata
graph and discovery; A2A streaming and push notifications; approving mapping fields from the console's review queue; the Wasm plugin host. The native Postgres, Salesforce and REST connectors run inside the Go
worker for the prototype; production connectors run on the Camel/Java worker types in §7.1.
