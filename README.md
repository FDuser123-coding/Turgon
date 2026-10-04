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
bin/turgon secrets runtime-spec.json                           # env vars (or OpenBao paths) for each secretRef
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

#### Change data capture over the Pub/Sub API

`salesforce-won-deals-to-erp-cdc` runs the same saga, but the won deal is pushed to the worker
as it happens instead of being polled. The connection subscribes to Salesforce's change events
(or to a platform event) over the Pub/Sub API (gRPC). It uses the same integration user and
token:

```yaml
events:
  - { name: Opportunity.Won, entity: Opportunity, interface: change-data-capture }
config:
  subscriptions:
    Opportunity.Won:
      topic: /data/OpportunityChangeEvent      # or /event/Order_Placed__e
      changeTypes: [UPDATE]
      match: { StageName: Closed Won }         # an update carries only what it changed
      fields: [AccountId, Amount, CloseDate, CurrencyIsoCode,
               "(SELECT Product2.ProductCode, Quantity FROM OpportunityLineItems)"]
```

- **Every event is kept, in order.** Each event is stored in Turgon's inbox together with the
  replay ID to resume after it, in one transaction. A worker that crashes or restarts
  resumes after the last stored event, with nothing lost and nothing read twice. Replay IDs
  are kept as the opaque bytes Salesforce sends.
- **Resuming after a long outage.** Salesforce keeps events for three days. After a longer
  outage the stored replay ID is rejected, and the subscription starts again from the oldest
  event Salesforce still holds. The inbox drops the events it already has.
- **One subscriber per event.** Workers take a lock in Postgres: one holds the subscription
  and the others stand by. When the holder dies, another worker takes over within seconds.
  Events are requested in batches (`batch`, 100 by default), and Salesforce's keepalives
  move the stored position forward even when nothing happens.
- **Payload.**
  - A change event becomes one event per changed record, with its `Id` and the
    `ChangeEventHeader`. The header's field bitmaps (`changedFields`, `nulledFields`,
    `diffFields`) are decoded to field names, including compound fields such as
    `Name.FirstName`.
  - Fields the change did not touch are left out, and fields it cleared are `null`.
  - With `fields` set, the payload is the record as it is now, read with SOQL. It then has
    the same shape as the polled event, so the same mapping serves both.
- **`turgon check`** asks the Pub/Sub API whether each topic exists and whether the
  integration user may subscribe to it.

The fake org also serves the Pub/Sub API (plaintext gRPC on `127.0.0.1:7443`; Turgon uses
TLS for every endpoint except a loopback one). Each deal won through `sf.cmds` is published
as a change event. In a catalog whose connection sets `pubsubEndpoint: 127.0.0.1:7443`:

```sh
bin/turgon compile -c my-catalog salesforce-won-deals-to-erp-cdc -o sf-cdc.json
bin/turgon run -s sf-cdc.json & bin/turgon run -s sf-cdc.json &   # one subscribes, one stands by
echo "001000000000001AAA 7800" >> sf.cmds                          # the run starts at once
```

#### Linking accounts at go-live with the Bulk API

A flow resolves each record to a master record: the Salesforce account behind a won deal to the
ERP customer. At go-live, most accounts already carry their ERP customer number, so there is no
need for a data steward to link them one by one. `turgon xref load` links them all at once from
an export the connection defines:

```yaml
config:
  exports:
    Account.ERPNumbers:
      sobject: Account
      fields: [Id, Name, ERP_Customer_Number__c]   # relationship fields (Owner.Email) too; no subqueries
      where: "ERP_Customer_Number__c != null"
      # all: true reads deleted and archived records too (queryAll)
```

```sh
bin/turgon xref load salesforce-prod Account.ERPNumbers -c my-catalog --entity Customer \
  --master ERP_Customer_Number__c --match Name:name --dry-run      # then again without --dry-run
```

- **The Bulk API 2.0.** The export runs as a query job. Turgon creates it, waits for it, then
  reads the CSV results a page at a time (50,000 records per call). It checks that it read as
  many records as the job processed, and deletes the job when done. An interrupted wait aborts
  the job. A job reads millions of records in a few API calls; the REST API needs one call per
  2,000.
- **Stewards' decisions are kept.**
  - A record already linked to another master record is listed and left alone. `--replace`
    moves it.
  - Records without a master value are counted and skipped.
  - `--dry-run` writes nothing and shows what would change.
- **Matching improves.** With `--match` (`FIELD:KIND`, kinds `email`, `domain`, `name` or
  `exact`), each record's identifying fields are kept. Later records from other systems are
  then matched against them.
- **Written in bulk and audited.** Links are written with `COPY`, 5,000 per transaction.
  Links that did not change are not rewritten. Against the fake org, 100,000 accounts load in
  5.4 s, of which about 3 s is waiting for the job, and a reload with nothing changed takes
  3.9 s. Each load is one `xref.loaded` audit entry, with the counts and a SHA-256 of the
  links it made.
- **`turgon check`** covers the export's fields: each must exist and be visible to the
  integration user.

With the fake org, `account <AccountId> <ERP number> <name>` on `sf.cmds` creates an account,
and `accounts 100000` creates many. After the load, a won deal for a loaded account resolves
without `turgon xref set`.

### Change capture from SQL Server, Oracle, MySQL and Db2: Debezium

Turgon reads Postgres' log itself (`logical-replication`, above). For the other databases,
[Debezium](https://debezium.io) reads the log (the SQL Server change tables, Oracle LogMiner,
the MySQL binlog, Db2 ASN) and writes each row change to a Kafka topic, and the `debezium`
connector consumes that topic. Debezium runs as Kafka Connect or as Debezium Server; Turgon
only needs the topic. `legacy-orders-to-erp` uses it: every open order inserted into a legacy
SQL Server system becomes an ERP sales order, and the legacy system is never queried or
written to.

```yaml
spec:
  connector: debezium@^1
  secretRef: openbao://legacy-erp/kafka          # {"username", "password"} with sasl
  events:
    - { name: Order.Placed, entity: Order, interface: debezium }
  config:
    brokers: [kafka-1.internal:9093, kafka-2.internal:9093]
    tls: true                                  # required unless the brokers are on this host
    sasl: scram-sha-512                        # or scram-sha-256, or plain (over TLS)
    events:
      Order.Placed:
        topic: legacy.dbo.Orders               # <topic.prefix>.<schema>.<table>
        operations: [create]                   # create, update, delete, read (snapshot rows)
        match: { status: open }
        # start: earliest reads what the topic still holds, the snapshot included
```

- **Nothing lost, nothing read twice.**
  - Changes are stored in Turgon's inbox in one transaction with the Kafka offsets to resume
    after them, the same as Salesforce subscriptions.
  - A new subscription first stores where it starts (the topic's end, or its start with
    `earliest`). A worker that restarts before the first change therefore does not skip one.
  - If Kafka deleted changes that were never read (retention), the subscription stops and says
    so instead of skipping them.
- **Replays are recognized.**
  - After Debezium restarts, it sends changes again from its last committed position.
  - Each change's ID comes from its position in the database log (SQL Server's
    `change_lsn`/`commit_lsn`/`event_serial_no`, Oracle's SCN, the binlog file and position,
    Postgres' LSN), plus the row's key and the operation.
  - The inbox therefore drops the second copy, even though it sits at another Kafka offset.
- **Values mean what the database stored.** With the JSON converter's schemas on (Kafka
  Connect's default), Connect's encodings are decoded:
  - `Decimal` and `VariableScaleDecimal` become exact numbers;
  - `Date` becomes a date;
  - `Timestamp`, `MicroTimestamp` and `NanoTimestamp` become UTC timestamps;
  - `Time` becomes a time of day;
  - `Json` columns become documents.
  Without schemas, set `decimal.handling.mode=string`.
- **The payload** is the row: after the change, or the deleted row for a delete. Under
  `debezium` it also carries:
  - the operation (`op`);
  - the table (`source`);
  - the time (`ts`);
  - for an update, the row before it (`before`), so a flow can react to a status changing.

  Tombstones are skipped. A flattened event (the `ExtractNewRecordState` transform) is refused
  with an explanation, because Turgon needs the envelope.
- **`turgon check`** connects to the brokers and finds each topic. If a topic is missing, it
  explains Debezium's naming and the Kafka ACLs the user needs.

Measured with Postgres 16, Debezium Server 3.7.0 (Kafka sink), Apache Kafka 4.3.1 (KRaft)
and two workers:
- **Normal run.** An order row inserted into the legacy database became a run with its
  `DECIMAL`, `DATE` and JSON columns decoded, and was written to the ERP after approval.
  Draft rows and the snapshot were left alone.
- **Workers killed.** After `kill -9` of the worker holding the subscription, 5 new rows gave 5
  events. With every worker down, 3 more rows were read when a worker came back.
- **Replays.** After Debezium's own `kill -9`, and with 4 changes replayed onto the topic, the
  inbox held 9 events with 9 distinct IDs.
- **Throughput.** A burst of 1,000 inserted rows reached the inbox in 3.3 s.

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
- **Auth**: a bearer token, an API-key header, basic, OAuth 2.0 client credentials (refreshed
  before expiry and after a 401), or OAuth 1.0a request signing with HMAC-SHA256 (NetSuite's
  token-based authentication), always from the connection's secret.
- **APIs that upsert by an external key** take the write's idempotency key in the path
  (`{{idempotencyKey}}`), quoted safely inside OData key literals. A retried write then finds its
  record. **APIs that answer 204 with no body** give the record's ID in a header
  (`idFrom: Location` or `OData-EntityId`), and the compensating delete is templated from it.
- **Creates are previewed** as the request they would send, so approvers see the exact body, and a
  payload missing a field fails before anyone approves.
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

### Dynamics 365 and NetSuite: ERPs through the REST connector

Two more ERPs need no new connector, only configuration of `rest`
(`examples/connections/dynamics-crm.yaml`, `netsuite-erp.yaml`):

- **Dynamics 365 (Dataverse Web API).**
  - **Auth:** an app registration's client credentials against Entra ID.
  - **Creating orders:** sales orders are upserted by an alternate key,
    `PATCH /salesorders(turgon_externalref='<key>')`, with the account bound by
    `customerid_account@odata.bind`. Lines are write-in products, so no product catalog is needed.
  - **Result:** the order's GUID comes from `OData-EntityId`.
  - **Events:** changed orders are polled on `modifiedon`.
- **NetSuite (SuiteTalk REST).**
  - **Auth:** token-based authentication (OAuth 1.0a, HMAC-SHA256, the account as realm).
  - **Creating orders:** sales orders are upserted by external ID,
    `PUT /salesOrder/eid:<key>`. Items are referenced by internal ID through a mapping table
    kept per deployment.
  - **Result:** the order's ID comes from `Location`.
  - **Events:** changed orders are read with SuiteQL (`Prefer: transient`).

`hubspot-won-deals-to-dynamics` turns won HubSpot deals into Dynamics orders and records the
order's GUID on the deal. `shopify-store-orders-to-netsuite` is the Shopify saga with NetSuite.
Without the systems, run the test fakes:

```sh
go build -o bin/fakedynamics ./internal/tools/fakedynamics    # http://127.0.0.1:9700, client turgon-app / demo
go build -o bin/fakenetsuite ./internal/tools/fakenetsuite    # http://127.0.0.1:9900, account 1234567_SB1
sed -i 's|https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token|http://127.0.0.1:9700/oauth2/v2.0/token|; s|https://contoso.crm4.dynamics.com|http://127.0.0.1:9700|' \
  my-catalog/connections/dynamics-crm.yaml
sed -i 's|https://1234567-sb1.suitetalk.api.netsuite.com|http://127.0.0.1:9900|' my-catalog/connections/netsuite-erp.yaml
export TURGON_SECRET_DYNAMICS_CRM_APP_REGISTRATION='{"clientId":"turgon-app","clientSecret":"demo"}'
export TURGON_SECRET_NETSUITE_ERP_TBA='{"consumerKey":"ck-demo","consumerSecret":"cs-demo","tokenId":"ti-demo","tokenSecret":"ts-demo"}'
bin/turgon xref set --entity Customer --system shopify-store --source ada@example.com --master 1001
bin/turgon xref set --entity Customer --system hubspot-crm --source ada@lovelace-gmbh.example \
  --master 6a1c0e2f-8b3d-4f5a-9c7e-1d2b3a4c5e6f
```

### SAP S/4HANA: sales orders through SAP's released APIs

The `sap-odata` connector reaches SAP S/4HANA (Cloud, and on-premise 1909 or later) only through
its released OData v2 APIs, the interfaces SAP sanctions for integration; the manifest prohibits
direct database access and ODP-RFC. Like `rest`, each Connection declares what it uses
(`examples/connections/s4-prod.yaml`):

- **Creates** send one deep insert, with the header and its items (`to_Item`). Values are converted
  to OData v2 types: ISO dates become `/Date(ms)/`, numbers become decimal strings.
- **Dry runs** go through the API's simulation service (`API_SALES_ORDER_SIMULATION_SRV`), which
  prices and checks the order without saving it. The approver sees SAP's own totals, not
  Turgon's guess.
- **No duplicates after a lost response.** The write's idempotency key goes into a reference
  property (`PurchaseOrderByCustomer`), and a create first looks for an order carrying it. A retry
  after a gateway timeout finds the order the first attempt made instead of creating a second.
- **Deletes** undo a create in a saga, with the order's ETag. SAP refuses once the order has
  follow-on documents, and the run then stops for a person.
- **Events** are entities polled on `LastChangeDateTime` with `$filter` and `$orderby`. As with
  Salesforce, a batch never ends inside a group of equal timestamps.
- **Reads** fetch one entity by key and return business names; they become agent tools.
- **Sessions:** modifying requests fetch and send SAP's CSRF token, and fetch a new one when the
  session expires. Authentication is a communication user (basic) or OAuth 2.0 client credentials.
- **Errors:** SAP's messages ("Sold-to party C-999 not maintained for sales area...") fail the
  step at once; throttling, locks and server errors are retried.

`turgon check` reads each service's `$metadata` and names every entity set or property that does
not exist. It also explains the fix for a service that is not activated or not in the
communication arrangement.

`shopify-store-orders-to-s4` is the Shopify saga with S/4HANA as the ERP. Without a system, run
the test fake of the three APIs:

```sh
go build -o bin/fakesap ./internal/tools/fakesap
touch sap.cmds && (tail -f sap.cmds | bin/fakesap -client 100 &)   # http://127.0.0.1:9600, TURGON_COMM / demo
sed -i 's|    baseURL: https://my300000-api.s4hana.cloud.sap|    baseURL: http://127.0.0.1:9600\n    client: "100"|' \
  my-catalog/connections/s4-prod.yaml                          # with the Shopify fake set up as above
bin/turgon compile -c my-catalog shopify-store-orders-to-s4 -o s4.json
export TURGON_SECRET_S4_PROD_COMM_USER=TURGON_COMM:demo
bin/turgon check -s s4.json                                     # services, entity sets, properties
bin/turgon run -s s4.json &
echo "ada@example.com 310.00" >> shop.cmds
```

The approval shows SAP's simulated order. Once approved, the fake prints the new sales order, and
the Shopify order's note gets its number.

### SAP ECC: BAPIs from a Camel/Java connector

Connectors with `runtime: camel-java` run outside the Go worker, in a sidecar container next to it,
and the worker drives them over the **connector protocol** (`proto/turgon/connector/v1`, gRPC on a
Unix socket in a volume only the pod mounts). The worker keeps everything that governs a write:
policy, approval, idempotency records, the rate governor and circuit breaker, audit, sagas. The
connector only talks to its system. So a connector can be written in the language its system needs,
and SAP ECC's RFC needs SAP JCo, which exists only for Java.

The protocol mirrors the worker's connector interfaces:

| Call | What it does |
|---|---|
| `Describe` | Names the connector, its version and capabilities |
| `Configure` | Binds an endpoint's compiled configuration and resolved secrets to an instance. A rotated secret configures a new one. |
| `Simulate`, `Commit`, `Confirm` | The write's dry run, the write, and reading it back |
| `Read`, `Poll`, `Export`, `Check` | Reads, polled events, exports and connection checks |
| `Stream` | Events the system pushes. The worker acknowledges each batch once its inbox holds it, and only then does the connector confirm the batch to the system |

Errors are status codes:

| Status | Meaning |
|---|---|
| `INVALID_ARGUMENT` | The payload is invalid |
| `FAILED_PRECONDITION` | The system refused the write |
| `NOT_FOUND` | The record is not there |
| `UNIMPLEMENTED` | The connector does not do this |
| `ABORTED` | The sidecar restarted: the worker configures the instance again and repeats the call |
| Anything else | Transient: the call is retried |

`connectors/` holds the Java side (Gradle, Java 21):

- `sdk` serves the protocol. A connector implements `Connector` and `Endpoint`, or extends
  `CamelEndpoint`, whose operations are Camel routes (`direct:<operation>`). The sidecar finds it
  with `ServiceLoader`.
- `sap-ecc` is SAP ECC 6.0 through BAPIs (`examples/connectors/sap-ecc.yaml`):
  - **create-sales-order** calls `BAPI_SALESORDER_CREATEFROMDAT2`, with `TESTRUN` for the preview.
    The idempotency key is the purchase order number, so a retried create finds the order with
    `BAPI_SALESORDER_GETLIST` instead of making a second. The create and `BAPI_TRANSACTION_COMMIT`
    run in one SAP session. The order is confirmed with `BAPI_SALESORDER_GETSTATUS`.
  - **cancel-sales-order** is its compensation. It deletes the order through
    `BAPI_SALESORDER_CHANGE`, which SAP refuses once there is a delivery.
  - **get-customer** calls `BAPI_CUSTOMER_GETDETAIL2`.
  - **Events** are the IDocs ECC sends (see below).
  - **Messages:** SAP's `RETURN` messages become the rejection's reason, such as
    `V1 462: Sold-to party 0000001002 is blocked for sales`.
- `sap-ecc-fake` is an in-memory ECC for tests and demos. It follows SAP's rules where they matter:
  an order is kept only if committed in the same session, and a test run saves nothing. A saved
  order is confirmed with an ORDRSP IDoc, which is sent again until accepted, as SM58 does. It is
  never part of the production image.

JCo is SAP's to license, so the connector calls it by reflection and the image does not carry it.
Download SAP Java Connector 3.1 from the SAP Support Portal and mount `sapjco3.jar` and
`libsapjco3.so` at `/opt/turgon/lib`. Without them, configuring the endpoint fails with that
instruction. The connection (`examples/connections/ecc-prod.yaml`) holds the RFC destination
(`ashost`, `sysnr`, `client`), the sales area and the order type. The secret is the RFC user,
`user:password`. The recipe is `salesforce-won-deals-to-ecc`: a won deal becomes an ECC order,
whose number is written back to the opportunity; if that fails, the order is deleted again.

```sh
make connectors                                                 # Java tests, then Go drives the sidecar
connectors/sap-ecc-fake/build/install/turgon-connector-sap-ecc-demo/bin/turgon-connector-sap-ecc-demo &
#   listens on unix:///var/run/turgon/connectors/sap-ecc.sock (TURGON_CONNECTOR_LISTEN to change)
export TURGON_CONNECTORS=sap-ecc=unix:///var/run/turgon/connectors/sap-ecc.sock
export TURGON_SECRET_SAP_ECC_PROD=TURGON:demo TURGON_SECRET_SALESFORCE_PROD_JWT="$(cat sf.creds)"
bin/turgon compile -c examples salesforce-won-deals-to-ecc -o ecc.json
bin/turgon check -s ecc.json                    # the sidecar, RFC_PING, each BAPI, the sales area
bin/turgon xref set --entity Customer --system salesforce-prod --source 001000000000001AAA --master 1000
bin/turgon run -s ecc.json &
echo "001000000000001AAA 7800" >> sf.cmds       # approve: an ECC order, its number on the opportunity
echo "reject 006000000000002AAA" >> sf.cmds     # the next deal's write-back fails...
echo "001000000000001AAA 900" >> sf.cmds        # ...and the saga deletes its ECC order
```

#### ECC events: IDocs over tRFC

ECC pushes events the standard way, as outbound IDocs. For example, output BA00 sends an order
confirmation (ORDRSP, basic type ORDERS05) when an order is saved. The connector receives them
as a server program registered at the SAP gateway.

On the ECC side:
1. In SM59, create an RFC destination of type T with *Registered Server Program* set to the
   connection's `progid`.
2. In WE21, create a tRFC port on that destination.
3. In WE20, create a partner profile for the receiving logical system, with outbound parameters for
   the message types.
4. Allow the program in the gateway's `reginfo`.

On the Turgon side, the connection maps message types to events:

```yaml
config:
  idoc:
    server: { gwhost: ecc.acme.internal, gwserv: sapgw00, progid: TURGON_IDOC, connectionCount: 2 }
    events:
      SalesOrder.Created: { messageTypes: [ORDRSP], idocTypes: [ORDERS05] }
```

The program is registered only while a worker holds the event's subscription; one worker holds it
at a time. Each IDoc travels like this:

1. SAP sends it over tRFC.
2. The sidecar passes it to the worker over `Stream`.
3. The worker stores it in its Postgres inbox and acknowledges.
4. Only then does the sidecar confirm the transaction to SAP.

The sidecar keeps nothing. If the worker is away, or cannot store the IDoc, the transaction fails
and SAP sends it again (SM58: set the destination's tRFC options to retry). The inbox drops an
IDoc it already has, by sender and IDoc number. IDocs of message types no event takes are accepted
and dropped.

Fields come from SAP's own segment definitions (`IDOCTYPE_READ_COMPLETE`, read once per IDoc type).
The event is the control record and the segments as a tree, which a mapping reads with JSONata:

```json
{"idoc": {"docnum": "0000000000004711", "mestyp": "ORDRSP", "idoctyp": "ORDERS05", "sndprn": "ECCCLNT100"},
 "segments": [{"segment": "E1EDK01", "fields": {"CURCY": "EUR", "BELNR": "0000012000"}, "segments": []},
              {"segment": "E1EDP01", "fields": {"POSEX": "000010", "MENGE": "2"},
               "segments": [{"segment": "E1EDP19", "fields": {"QUALF": "002", "IDTNR": "M-7"}}]}]}
```

The example recipe is `ecc-orders-to-erp`, with mapping `ecc-order-confirmation-to-sales-order`:
an order saved in ECC becomes an order in the warehouse ERP database. Run it next to
`salesforce-won-deals-to-ecc`, with both workers on the same sidecar. A won deal becomes an ECC
order, and the fake ECC confirms it with an ORDRSP IDoc, which then reaches the ERP. Stop the
second worker and the IDoc waits in the fake's SM58 queue until the worker is back.

In the Helm chart, `connectors.sidecars` adds a connector's container to every worker and agent pod,
with the socket volume and `TURGON_CONNECTORS`. With the operator, its Integrations get it too.

```yaml
connectors:
  sidecars:
    - name: sap-ecc
      image: { repository: ghcr.io/<owner>/turgon-connector-sap-ecc }
      volumes: [{ name: sapjco, persistentVolumeClaim: { claimName: sapjco } }]
      volumeMounts: [{ name: sapjco, mountPath: /opt/turgon/lib, readOnly: true }]
```

#### Business events through SAP Event Mesh

S/4HANA publishes business events, such as `sap.s4.beh.salesorder.v1.SalesOrder.Created.v1`, to
SAP Event Mesh: in S/4HANA Cloud through communication scenario SAP_COM_0092, on premise through
an event channel (`/IWXBE/CONFIG`). A connection can take them from an Event Mesh queue instead of
polling. The events arrive in seconds, and no port is opened: Turgon only calls out, to the
instance's REST messaging API.

```yaml
events:
  - { name: SalesOrder.Created, entity: SalesOrder, interface: event-mesh }
config:
  eventMesh:                                  # from the instance's service key
    url: https://enterprise-messaging-pubsub.cfapps.eu10.hana.ondemand.com   # messaging, protocol httprest: uri
    tokenURL: https://acme.authentication.eu10.hana.ondemand.com/oauth/token # oa2.tokenendpoint
    secretRef: openbao://s4-prod/event-mesh   # the oa2 section as it stands: {"clientid", "clientsecret", ...}
  subscriptions:
    SalesOrder.Created:
      queue: acme/s4/turgon/salesorders       # subscribed to .../ce/sap/s4/beh/salesorder/v1/SalesOrder/Created/v1
      types: [sap.s4.beh.salesorder.v1.SalesOrder.Created.v1]   # or a prefix: sap.s4.beh.salesorder.v1.*
      match: { SalesOrderType: OR }
      read: { service: API_SALES_ORDER_SRV, entitySet: A_SalesOrder, key: SalesOrder, expand: [to_Item] }
```

- **Nothing is lost.**
  - Each message is taken with QoS 1 and acknowledged only after its event is stored in
    Turgon's inbox.
  - If a worker stops between the two, Event Mesh delivers the message again, and the inbox
    recognizes its CloudEvent ID.
  - While no worker runs, events wait on the queue.
  - A message that cannot be read (not a CloudEvent) is left unacknowledged. Give the queue a
    dead message queue and a redelivery limit, so such a message is set aside.
- **The payload.**
  - An S/4HANA event carries only the object's key and a few fields. With `read`, the payload
    is the object as the OData API reads it now, with the items. An object that was deleted
    keeps the event's data.
  - The event's `id`, `type`, `source` and `time` are under `cloudEvent`.
  - Other types on the queue are acknowledged and dropped.
- **One consumer per queue.** Workers take the same lock as for Salesforce subscriptions: one
  consumes and the others stand by.
- **Throughput.** Messages are taken one at a time. Against the local fakes, 50 orders were
  stored in 0.8 s; with real network round trips, expect roughly 5 to 20 events a second per
  queue.
- **Credentials.** The instance's OAuth client is sent in the token request's form, because
  XSUAA takes Basic credentials as they stand and its client IDs contain `!` and `|`. Only a
  deployment whose flows take events from Event Mesh needs it: one that only writes to SAP
  (`shopify-store-orders-to-s4`) runs without it.
- **`turgon check`** signs in to the instance. Queues cannot be inspected without taking their
  messages, so a missing queue shows when the subscription opens (`404 ... is there a queue
  acme/s4/turgon/salesorders on the instance?`).

`s4-sales-orders-to-dynamics` uses it: an order created in SAP (by inside sales, EDI, or another
flow) becomes a Dynamics 365 sales order for the customer's account. The SAP customer number is
linked to the account once, by a data steward (resolve `exact`). With `-mesh`, the SAP fake also
runs a fake Event Mesh instance and publishes its sales order events to the queue:

```sh
touch sap.cmds && (tail -f sap.cmds | bin/fakesap -client 100 -mesh 127.0.0.1:9601 &)
sed -i 's|https://enterprise-messaging-pubsub.cfapps.eu10.hana.ondemand.com|http://127.0.0.1:9601|;
        s|https://acme.authentication.eu10.hana.ondemand.com/oauth/token|http://127.0.0.1:9601/oauth/token|' \
  my-catalog/connections/s4-prod.yaml                          # with S/4HANA and Dynamics set up as above
bin/turgon compile -c my-catalog s4-sales-orders-to-dynamics -o s4dv.json
export TURGON_SECRET_S4_PROD_EVENT_MESH='{"clientid":"sb-turgon!b1|xbem-service-broker!b2","clientsecret":"demo"}'
bin/turgon xref set --entity Customer --system s4-prod --source C-100 --master 6a1c0e2f-8b3d-4f5a-9c7e-1d2b3a4c5e6f
bin/turgon run -s s4dv.json & bin/turgon run -s s4dv.json &   # one consumes, one stands by
echo "order C-100 M-2 4" >> sap.cmds                           # an order entered in SAP: the run starts at once
```

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

### Postgres change capture: events without an outbox

An event can also be read from the database's write-ahead log, so the application needs no
outbox: every committed insert, update or delete of a table is an event. The connection names
the table under `changes` and gives the event the `logical-replication` interface
(`examples/connections/shop-db.yaml`, recipe `shop-order-rows-to-erp`):

```yaml
events:
  - { name: Order.Placed, entity: Order, interface: logical-replication }
config:
  changes:
    Order.Placed: { table: shop.orders, operations: [insert] }   # also update, delete
```

- **Built into Postgres.** The worker reads the log with the `pgoutput` plugin that ships with
  Postgres 10+, over an ordinary connection. It needs no Debezium, no Kafka and no extension.
  The database needs `wal_level = logical` and a user with the `REPLICATION` attribute;
  `turgon check` tests both and says how to fix them.
- **One publication and one slot per event.** They are named `turgon_<event>` and created on
  the first poll. The publication holds only the table and the event's operations. Turgon
  refuses to publish updates or deletes of a table without a primary key or replica
  identity, because Postgres would then reject the application's own updates.
- **Commit order, no loss, no duplicates.** Events come in commit order: a transaction that
  wrote first but committed last comes last, and is not skipped. The slot is only peeked.
  It advances to what the dispatcher has confirmed, so a worker killed mid-transaction reads
  the rest of it again, and the run IDs start each run once.
  Several workers can poll the same slot: while one reads it, the others wait their turn.
- **Payload.** The payload is the row as JSON: numbers, booleans and `jsonb` keep their
  types, and timestamps are RFC 3339. A delete carries the replica identity, which by default
  is the primary key.
- **The log it holds.** A slot keeps write-ahead log until it advances. Turgon also releases it
  when the table is quiet but the database is busy.
  `turgon_cdc_retained_wal_bytes` shows what each slot holds, and the chart alerts above
  `monitoring.rules.cdcRetainedWALMiB` (4 GiB). Set `max_slot_wal_keep_size` so that a stopped
  worker cannot fill the disk. Drop the slot of an event you no longer use.

```sh
psql "$DB" -c "ALTER SYSTEM SET wal_level = logical"      # then restart Postgres
bin/turgon compile -c examples shop-order-rows-to-erp -o cdc.json
bin/turgon check -s cdc.json                               # wal_level, REPLICATION, the table
bin/turgon run -s cdc.json &
psql "$DB" -c "insert into shop.orders (order_number, total, currency, customer, items)
  values (3001, 349.90, 'eur', '{\"email\": \"ada@example.com\"}', '[{\"sku\": \"M-7\", \"qty\": 1}]')"
bin/turgon pending shop-order-rows-to-erp/000000002DCB2480.1   # run ID: commit LSN.change
```

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

`turgon console` serves the web console. Its home page, **Integrations**, maps what is connected
to what, drawn from `--catalog`:

- a map of the systems that send events, linked through Turgon to the systems it writes to;
- each flow step by step: the event and how it arrives (webhook, polling, outbox, change capture
  or subscription), the customer match, and each write with its risk, dry run, approval and undo;
- the flow's live runs, approvals waiting and failures.

`turgon integrations -c <catalog>` prints the same map in a terminal.

The other pages are the approval queue (each pending write with its
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
    strategy: probabilistic      # or splink (below), or exact: never links on its own, but stewards get suggestions
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
conservative defaults for customer data until `turgon identity train` estimates them from the
deployment's links (below).

**The Splink service.** With `strategy: splink`, records are scored by
[Splink](https://moj-analytical-services.github.io/splink/) instead, run as a service next to the
workers (`deploy/splink`, image `ghcr.io/<owner>/turgon-splink`). It reads the confirmed links
from `turgon_xref`, trains a model per entity on them (m from the links, u from pairs of different
master records, both drawn towards the built-in model's values while links are few), and retrains
when the links change, so each steward decision improves the next match. Comparisons are the
built-in ones: the email address, else the company domain, the name by Jaro-Winkler, exact fields.
Turgon still decides: a certain match is linked and audited (`xref.matched` with the model, say
`splink (240 records of 180 masters)`), anything else goes to the steward queue with Splink's
suggestions. An entity without links yet, or workers without `TURGON_SPLINK_URL`, send records to
the steward with the built-in suggestions; while the service is unreachable the step is retried.

```sh
docker run -d -p 8095:8080 -e TURGON_SPLINK_DATABASE_URL=postgres://splink_ro@db/turgon \
  -e TURGON_SPLINK_TOKEN=s3cret ghcr.io/<owner>/turgon-splink:<version>
export TURGON_SPLINK_URL=http://127.0.0.1:8095 TURGON_SPLINK_TOKEN=s3cret
bin/turgon check -s hubspot.json          # ok  splink Customer: 8 records of 5 masters, trained: m, u
bin/turgon run -s hubspot.json
```

| Setting | |
|---|---|
| `TURGON_SPLINK_DATABASE_URL` | Turgon's database; read access to `turgon_xref` is enough |
| `TURGON_SPLINK_TOKEN` | bearer token workers must send (`TURGON_SPLINK_TOKEN` on the workers too) |
| `TURGON_SPLINK_RETRAIN` | seconds between checks for changed links (300) |
| `TURGON_SPLINK_PRIOR` | prior that a record and a blocked candidate are the same entity (0.05) |

The API is `POST /v1/match` (`{"entity", "attributes", "limit"}` → candidates with probability,
match weight and reasons), `POST /v1/train` and `GET /healthz` (models per entity, no record data).
Attributes are personal data: the service logs counts only. In the Helm chart, `splink.enabled`
deploys it with a NetworkPolicy that admits only Turgon's pods, and points the workers at it.

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
  an artifact, or `rejected` / `failed` with the reason. Only the agent that asked can see it,
  and a person, not the agent, decides it (`tasks/cancel` is refused).
- A write's `requestId` defaults to the message ID, so resending a message never writes twice.

An agent follows a write in one of three ways:

- **Polling** with `tasks/get`.
- **Streaming.** With `message/stream` (or `tasks/resubscribe` later), the stream sends the task,
  then a status update for each change.
  - The outcome arrives the moment it happens: Temporal answers the wait as soon as the write
    ends.
  - On completion the result comes as an artifact before the final status.
  - Keepalives every 15 seconds hold the stream open through proxies.
  - A stream ends after `--a2a-stream-limit` (an hour) without a final event. The agent then
    resubscribes.
  - A read streams its reply as a single message.
- **Push notifications.** The agent gives a URL in the message's `pushNotificationConfig`, or
  later with `tasks/pushNotificationConfig/set`. Turgon then POSTs the task when the write waits
  for approval and when it ends.
  - Each POST carries the agent's `token` (`X-A2A-Notification-Token`), and its credentials as
    `Authorization: Bearer` or `Basic` if it gave some.
  - The write's workflow keeps the URLs (at most five), so they survive restarts and any MCP
    replica sees them.
  - `turgon run` workers send the notifications, and retry them for a few minutes.
  - The outcome is sent from a separate workflow, so a receiver that is down never delays it
    for agents that poll or stream.
  - `get` and `list` never return the credentials. They are kept in the write's Temporal
    history, encrypted when payload encryption is on (`TURGON_PAYLOAD_KEYS`).
  - Writes started before this release still complete. They send no notifications: recorded
    histories replay in the tests.

**Where notifications may go.** Push URLs come from agents, but the requests leave from inside
the customer's network.
- Turgon only calls `https` URLs whose addresses are all public. It refuses loopback, private,
  link-local (cloud metadata), CGNAT, NAT64/6to4 and reserved ranges. It checks when the URL is
  set, and again on every connection, so a name that later resolves to an internal address is
  still refused.
- Redirects are not followed, and no egress proxy is used.
- Agent platforms inside the network can be allowed by CIDR with `TURGON_A2A_PUSH_ALLOW` (Helm
  `agents.pushAllow`). Plain `http` is accepted only for those ranges.

Behind agentgateway each spec's agent is at `/<spec>/a2a`, with the same token authentication
and identity headers; the gateway rewrites the card's URL to its own.

### Logic plugins: sandboxed WebAssembly

A logic plugin (§18.4) is customer- or partner-written code that reacts to what Turgon writes.
It runs as WebAssembly in the worker, isolated from everything but the calls an administrator
granted it. `credit-check` is the example. It is installed next to a recipe
(`shop-orders-with-credit-check`) or in a stack blueprint (`eu-distributor-core`).

When a sales order is committed, `credit-check`:
- reads the customer's credit limit;
- asks the risk service at `api.acme-risk.example` for the customer's score, with an API key
  Turgon puts in for it;
- proposes the order's credit status: `approved` within the limit and with a score of 50 or
  more, `review` otherwise (if the risk service does not answer, the limit decides alone);
- publishes `credit.checked` for other plugins.

```yaml
# examples/plugins/credit-check.yaml          # examples/recipes/shop-orders-with-credit-check.yaml
spec:                                         spec:
  type: logic                                   ...
  runtime: wasm                                 extensions: [credit-check@1.3]
  world: turgon:stack/logic-plugin@0.2.0
  module: credit-check/credit-check.wasm
  subscribes: [model.SalesOrder.created]
  permissions:
    entities: { read: [Customer], propose: [SalesOrder.creditStatus] }
    network:  { allow: ["api.acme-risk.example"] }
    secrets:  [acme-api-key]        # a handle, never the value
    events:   { publish: [credit.checked] }
  limits: { memoryMB: 64, timeoutMs: 5000 }
```

- **The interface** is the WIT world in `wit/turgon-stack.wit`, version 0.2.0. Plugins built
  for 0.1.0 (`wit/0.1.0/`, no network or secrets) still run unchanged.
  - The plugin exports `handle(event)`. It receives the event as JSON: its `type`, the
    `entity`, its `id`, and the written `record` (or the `payload` of an event another plugin
    published).
  - It may import:
    - `entities.get` and `entities.propose-change`;
    - `events.publish`;
    - (0.2.0) `http.send`.
  - Errors are `denied`, `not-found`, `invalid(reason)` and (0.2.0) `unavailable(reason)`
    for a service that did not answer. A 0.1.0 plugin sees `unavailable` as `invalid`.
  - Build it with wit-bindgen for `wasm32-unknown-unknown`: the example is Rust
    (`examples/plugins/credit-check/`). A component made with `wasm-tools component new` works
    too.
- **The sandbox.** Turgon runs plugins with wazero, in-process.
  - A plugin gets no WASI: no files, clock, randomness or environment. A module that imports
    anything outside the world is refused.
  - Each event runs in a fresh instance, within `memoryMB` and `timeoutMs`; HTTP requests
    count against the same time.
  - Invocations are capped at 64 host calls, 16 HTTP requests, 16 published events and 1 MiB
    per argument or response.
  - A plugin that traps, loops or runs out of memory fails its invocation only. The run's
    write stands, and the next event starts clean.
- **Grants, enforced on every call.**
  - `get` reads an entity the plugin may read. Reads go through the write guard with the
    plugin's identity (`plugin:credit-check@1.3.0`), so policy, the rate governor, the
    circuit breaker and the audit log apply.
  - `propose-change` takes a JSON patch of fields the plugin may change
    (`SalesOrder.creditStatus` allows only `creditStatus`). It becomes a governed write:
    policy decides, and a high-risk or large change waits for a person in the console like an
    agent's write. A proposal is idempotent per run, entity and change.
  - `http.send` reaches only the hosts in `network.allow` (port 443 unless a grant names
    another), over https.
    - Turgon checks every address the host resolves to and refuses private, loopback and
      link-local ones (cloud metadata included), at every connection.
    - There are no redirects and no proxy. `TURGON_PLUGIN_NETWORK_ALLOW` (CIDRs) lets
      plugins reach internal ranges; plain `http` is accepted only there.
    - A plugin cannot set `Host`, `Content-Length` or hop-by-hop headers.
    - Error responses come back as responses; the plugin decides.
  - **Secrets.** A header value may name a granted secret as `{{secret:acme-api-key}}`, and
    Turgon puts the value in on the way out.
    - The plugin never holds the secret, and the audit log records only the hosts called,
      never paths, headers or bodies.
    - The value is read with the deployment's secret backend from
      `openbao://plugins/<plugin>/<handle>`, for example
      `TURGON_SECRET_PLUGINS_CREDIT_CHECK_ACME_API_KEY` with environment variables, or the
      field `acme-api-key` of the secret `plugins/credit-check` in OpenBao or a cloud secret
      manager. It rotates like connection secrets.
    - Grant network access only to hosts you trust with the secret: a host that echoed
      request headers back would hand it to the plugin.
  - `events.publish` sends only granted topics. Plugins subscribe to them as
    `plugin.<publisher>.<topic>` (`plugin.credit-check.credit.checked`).
    - Published events are delivered after the publishing invocation succeeds, to every
      subscribed plugin in the spec, up to three plugins deep.
    - A plugin never handles an event its own chain started. A subscriber's failure is
      audited and does not fail the publisher.
  - Anything else answers `denied`.
- **Which operations serve them.** By convention, `get-<entity>` reads and
  `update-<entity>` applies proposals, on a connection that declares the entity: `erp-db`
  gains `update-sales-order`, which sets `credit_status`. The verifier warns when an entity
  has no such operation.
- **When plugins run.** After a write step commits, every plugin subscribed to the model event
  the write causes (`model.SalesOrder.created`, `.updated`, `.deleted`, by operation name)
  runs once with the written record. A redelivered event that finds the write already done runs
  nothing again. Runs started by older workers are unaffected (a recorded history replays in
  the tests).
- **The code is part of the spec.** The compiler embeds the module and its SHA-256 in the
  runtime spec, so the spec's digest and signature cover the plugin's code. Workers check the
  digest before loading.
  - `turgon verify` loads each module within its limits, reports its world and imports, and
    refuses a module built for another world than its manifest says.
  - `turgon check` loads each plugin, reads its secrets and checks its hosts resolve to
    addresses it may reach.
  - `turgon secrets` lists the plugins' secrets with the connections'.
- **Reproducible modules.** `scripts/build-plugins.sh` rebuilds the committed modules with a
  pinned Rust toolchain and remapped paths. CI checks that they match their sources.
- **Verified live**, with the real binary, Temporal, Postgres and a stand-in risk service at
  `https://api.acme-risk.example` (a local DNS entry and its own CA). The credit limit was
  1000.
  - 500 with a score of 80: `approved`.
  - 500 with a score of 30: `review`.
  - 500 with the risk service down: `approved`, on the limit alone.
  - 1500: `review`.
  - Each invocation took about 40–70 ms, published `credit.checked`, and recorded only the
    host it called. The API key reached the risk service and appeared nowhere in the logs or
    the audit log.
  - `turgon check` read the plugin's secret and checked its host.

### Install on Kubernetes

The chart in `deploy/helm/turgon` installs one worker per compiled spec and the console into
a single namespace: no cluster roles, no API server tokens, the "restricted" pod security
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

#### Connection secrets from OpenBao or Vault

Every `secretRef` in a spec (`openbao://shop-db/dsn`) can be read from OpenBao or HashiCorp
Vault instead of environment variables. The reference is the key `dsn` of the KV v2 secret at
path `shop-db`, read from `secret/data/shop-db`; `vault://` works too. A value may be a string
or a JSON object; Salesforce's credentials are then kept as fields.

```sh
bao kv put -mount=secret shop-db dsn='postgres://...'
bao policy write turgon-read - <<'EOF_POLICY'
path "secret/data/shop-db" { capabilities = ["read"] }
path "secret/data/erp-db"  { capabilities = ["read"] }
EOF_POLICY
bao write auth/kubernetes/role/turgon bound_service_account_names=turgon \
  bound_service_account_namespaces=integrations audience=openbao token_policies=turgon-read token_ttl=1h
helm upgrade turgon deploy/helm/turgon -n integrations --reuse-values \
  --set secrets.backend=openbao --set secrets.openbao.address=https://openbao.openbao.svc:8200 \
  --set secrets.openbao.caSecret=openbao-ca     # a Secret with ca.crt, if the server's CA is private
```

- **How pods sign in.**
  - With `kubernetes` auth, the default, each pod mounts a service account token for OpenBao
    alone. It is projected, bound to the audience `openbao`, valid for an hour and rotated by
    the kubelet.
  - The pods still mount no API server token.
  - The role must name the release's service account (the release's full name, `turgon`
    here), its namespace, and the audience.
  - `approle` (`secrets.openbao.existingSecret` with key `secret-id`) and `token` (key
    `token`) are the alternatives. On the appliance, set the `TURGON_OPENBAO_*` variables in
    `/etc/turgon/turgon.env`.
- **Least privilege.** Turgon only reads `<mount>/data/<path>`. The policy can name each path
  a spec uses, and `turgon secrets <spec>` lists them.
- **`turgon check`** (and `helm test`) signs in and reads every reference. Each failure says
  what to fix:
  - a role that does not bind the pod;
  - a policy without read on the path;
  - a secret that was never written, or whose latest version was deleted (restore it with
    `kv undelete`).

  Values never appear in checks, logs or the audit log.
- **Caching.** Values are cached for five minutes (`TURGON_OPENBAO_TTL`). Login tokens are
  replaced at two thirds of their lease, or at once if revoked.
- **Rotation without a restart.** Workers read their secrets again every
  `--secrets-refresh` (5 minutes by default). When one changed, they reconnect the
  connectors that use it:
  - The new connections are built next to the current ones and checked first.
  - If the new connections fail a check the current ones pass (a wrong password was
    written), the worker keeps working with what it has. It reports the refusal once, as
    `connections.reload-refused` in the audit log, and tries again at the next check.
  - Otherwise the worker lets its running activities finish (up to a minute; Temporal
    retries the rest, and writes are idempotent) and switches over. Runs waiting for an
    approval, subscriptions and the webhook listener carry on. The switch is audited as
    `connections.reloaded`.
  - `kill -HUP` reconnects at once.
  - `turgon mcp` does the same for the connectors of its read tools, with the same checks,
    audit entries and flag. New requests go to the new connections at once. Requests in
    flight finish on the old ones, which are closed afterwards. Agents see no error. MCP
    runs stateless, and A2A tasks are Temporal workflows, so no session is lost. Rate
    limits and circuit breakers start afresh with the new connections.
- **Settings.** `TURGON_SECRETS=openbao` and `TURGON_OPENBAO_{ADDR, MOUNT, NAMESPACE, CACERT,
  AUTH, ROLE, AUTH_MOUNT, JWT_FILE, SECRET_ID_FILE, TOKEN, TTL}`. `BAO_*` and `VAULT_*`
  (`BAO_ADDR`, `VAULT_TOKEN`, ...) are read as fallbacks. HTTPS is required unless the server
  is on the same host.

Verified live against Vault 2.1.1 and OpenBao 2.7.0 dev servers (`go test -tags live`, see
`pkg/secrets/live_test.go`):
- **Kubernetes auth**, with a real kube-apiserver that issues the projected token and answers
  OpenBao's TokenReview. A token for another audience, or from an unbound namespace, is
  refused.
- **AppRole**, and **`shop-orders-to-erp` run end to end** with both DSNs read from Vault.
  After approval the order reached the ERP, with no DSN or token in the logs or the audit log.
- **Rotation**, with Postgres checking passwords (SCRAM) for the connection's role:
  - A run waited for approval while the password was changed in Postgres, existing
    sessions were cut, and the new password was written to Vault. Within one refresh the
    worker reconnected, and the approved write went through with the new password.
  - A wrong password written to Vault was refused once and retried. Orders kept flowing with
    the current connections meanwhile.
  - A rotation written to the two secrets six seconds apart reconnected each connector as
    its secret arrived.
- **`turgon mcp` rotation**, with Secrets Manager on moto and the same Postgres setup:
  - An agent read `get_sales_order` every 100 ms while the password was changed in
    Postgres and in the secret manager. All 150 reads succeeded across the switch.
  - Afterwards only the new connections were open, and reads still worked once all of
    them were cut.
  - A wrong password was refused once, and reads kept working. `SIGHUP` reconnected.
    No password appeared in the log or the audit log.

#### Connection secrets from AWS, Azure or Google Cloud

On a cloud, the same references can be read from the cloud's own secret manager:
`secrets.backend` is `aws` (Secrets Manager), `azure` (Key Vault) or `gcp` (Secret Manager).

**Where each reference is read.** A connection's secret is one secret holding a JSON object.
`openbao://shop-db/dsn` is the field `dsn` of the secret named `<prefix>shop-db`. Key Vault and
Secret Manager do not allow `/` in names, so `team/erp-db` becomes `team--erp-db`. `turgon
secrets <spec>` prints each secret's name. The scheme of a reference is only a label, so
catalogs keep their references whichever manager a deployment uses.

```sh
aws secretsmanager create-secret --name turgon/shop-db --secret-string '{"dsn": "postgres://..."}'
helm upgrade turgon deploy/helm/turgon -n integrations --reuse-values \
  --set secrets.backend=aws --set secrets.aws.region=eu-central-1 --set secrets.prefix=turgon/ \
  --set serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::123456789012:role/turgon
```

**Identity.** Pods sign in with the platform's workload identity, bound through
`serviceAccount.annotations`:

| Backend | Identity | Service account annotation | Access the identity needs |
|---|---|---|---|
| `aws` | IRSA or EKS Pod Identity | `eks.amazonaws.com/role-arn` | `secretsmanager:GetSecretValue` on the secrets, `kms:Decrypt` on their key if it has its own |
| `azure` | AKS workload identity (the chart labels the pods `azure.workload.identity/use`) | `azure.workload.identity/client-id` | the Key Vault Secrets User role |
| `gcp` | GKE Workload Identity | `iam.gke.io/gcp-service-account` | `roles/secretmanager.secretAccessor` |

Outside Kubernetes (the appliance, a VM), the SDKs' default chains apply: an instance role or
managed identity, or `AWS_*`, `AZURE_*` and `GOOGLE_APPLICATION_CREDENTIALS`.

**Checks and rotation.**
- `turgon check` reads every reference. It says how to create a missing secret, and which
  permission is missing if access is denied.
- A secret holding a plain value instead of a JSON object is refused, with the reason.
- Values are cached for five minutes (`TURGON_SECRETS_TTL`).
- Rotations reach running workers and `turgon mcp` as they do with OpenBao
  (`--secrets-refresh`, `SIGHUP`).

**Settings.**

| Variable | Used by |
|---|---|
| `TURGON_SECRETS` = `aws`, `azure` or `gcp`; `TURGON_SECRETS_PREFIX` | all three |
| `TURGON_AWS_REGION`, `TURGON_AWS_SECRETS_ENDPOINT` (a VPC endpoint) | AWS |
| `TURGON_AZURE_VAULT_URL`, `TURGON_AZURE_API_VERSION` (for clouds that lag behind, such as Azure Stack Hub) | Azure |
| `TURGON_GCP_PROJECT`, `TURGON_GCP_SECRETS_ENDPOINT` | Google Cloud |

**Verification.** Verified locally against emulators, with the real `turgon` binary:
- **AWS**, with moto: the SDK's SigV4 client read the DSNs, `turgon check` passed,
  `shop-orders-to-erp` ran, and a password rotation in Postgres and Secrets Manager reconnected
  the worker. The run waiting for approval then wrote with the new password.
- **Azure**, with Lowkey Vault: `DefaultAzureCredential` got a managed identity token and
  followed Key Vault's authentication challenge. The same run and rotation passed.
- **Google Secret Manager** is covered by unit tests against a fake of its REST API only.
- None of the three has been tested against a real cloud account yet.

#### Temporal: TLS and encrypted payloads

Everything Turgon passes through Temporal (events, mapped records, write requests and results,
failures) is stored in Temporal's database and shown in its UI. Set `TURGON_PAYLOAD_KEYS` to
encrypt it with AES-256-GCM before it leaves Turgon: a comma-separated list of `id:key`, each key
32 random bytes in base64 (`head -c 32 /dev/urandom | base64`). The first key encrypts; every key
listed decrypts, so to rotate put a new key first and drop an old one once no run you still need
used it. Turgon's workers, console and MCP servers must share the keys. Temporal's own UI then
shows ciphertext; the console decrypts. Failure messages are encrypted too, and the messages of
map and resolve steps never quote a record (what they would quote is in the encrypted details).

Connect to Temporal over TLS with `--temporal-tls`, a CA with `--temporal-ca`, mutual TLS with
`--temporal-cert` and `--temporal-key`, and to Temporal Cloud with `TURGON_TEMPORAL_API_KEY`
(each flag also reads `TURGON_TEMPORAL_*`). In the chart:

```sh
kubectl -n integrations create secret generic turgon-temporal \
  --from-literal=payloadKeys="2026-09:$(head -c 32 /dev/urandom | base64)"   # and apiKey=... for Temporal Cloud
helm upgrade turgon deploy/helm/turgon -n integrations --reuse-values \
  --set temporal.existingSecret=turgon-temporal \
  --set temporal.tls.enabled=true --set temporal.tls.existingSecret=temporal-mtls \
  --set temporal.tls.caKey=ca.crt --set temporal.tls.certKey=tls.crt --set temporal.tls.keyKey=tls.key
```

#### Metrics and alerts

Every process serves Prometheus metrics at `/metrics`. Workers serve them on their health port,
and the console and MCP servers use `--metrics-listen`.

| Metric | What it counts |
|---|---|
| `turgon_runs_finished_total{workflow,outcome,reason}` | Finished runs; `reason` is why a run failed (`TurgonInvalid`, `TurgonUnresolved`, `TurgonCompensationFailed`, …). |
| `turgon_run_active_seconds{workflow}` | A run's duration **without** the time it waited for people. Recipe latency SLOs use this, so they measure Turgon and the target systems, not approvers. |
| `turgon_approvals_total{workflow,decision}`, `turgon_approval_wait_seconds` | Decisions (approved, rejected, timed_out) and how long people took. |
| `turgon_writes_total{target,operation,status}`, `turgon_write_duration_seconds` | Governed writes by outcome (committed, duplicate, denied, rejected, failed, compensated), and target latency. |
| `turgon_breaker_open{target}` | 1 while a target's circuit breaker refuses writes. |
| `turgon_events_total{workflow,via}`, `turgon_poll_errors_total`, `turgon_last_poll_success_timestamp_seconds` | Events read by polling or from the webhook inbox, and whether polling works. |
| `turgon_webhooks_total{endpoint,event,result}` | Deliveries: accepted, ignored, unauthenticated, invalid. Only configured endpoints become labels. |

The Temporal SDK's own metrics (`temporal_*`: task latencies, pollers, workflow outcomes by task
queue) go to the same endpoint.

With `--set monitoring.podMonitor.enabled=true --set monitoring.rules.enabled=true`, the chart
adds a PodMonitor and a PrometheusRule. The rule includes **one latency alert per recipe**, from
the `slo.p95Latency` compiled into its spec, plus these alerts:

- Runs failing above a ratio.
- A saga that could not undo its writes (critical).
- Records waiting for a data steward.
- Approvals timing out.
- Stalled polling.
- An open circuit breaker.
- Bursts of forged webhooks.

CI checks the rules with `promtool` and tests the alerts against synthetic series
(`deploy/helm/turgon/ci/alerts-test.yaml`).

#### Backup and restore

Turgon's state lives in its Postgres database: idempotency records, cursors, the webhook inbox,
cross-references and the audit log. Temporal keeps runs in its own database, which you back up
with Temporal.

With the chart's CloudNativePG cluster, enable continuous backup to S3-compatible storage. It
uses the Barman Cloud plugin, installed next to the operator. WAL is archived continuously and a
base backup is taken daily:

```sh
kubectl -n integrations create secret generic turgon-backup-s3 \
  --from-literal=ACCESS_KEY_ID=... --from-literal=ACCESS_SECRET_KEY=...
helm upgrade turgon deploy/helm/turgon -n integrations --reuse-values \
  --set database.cloudNativePG.backup.enabled=true \
  --set database.cloudNativePG.backup.destinationPath=s3://turgon-backups/prod \
  --set database.cloudNativePG.backup.credentialsSecret=turgon-backup-s3
```

To restore, install a release whose cluster is created from the backup. Omit `targetTime` to
restore the latest state, or give one to restore to that point:

```sh
helm install turgon-restored deploy/helm/turgon -n integrations -f my-values.yaml \
  --set database.cloudNativePG.recovery.enabled=true \
  --set database.cloudNativePG.recovery.serverName=turgon-turgon-db \
  --set 'database.cloudNativePG.recovery.targetTime=2026-09-28 10:00:00+00' \
  --set database.cloudNativePG.backup.serverName=turgon-turgon-db-2   # archive under a new name
```

The chart refuses a restored cluster that would archive over the backup it came from.

After any restore, check the audit log's hash chain and compare its head with one recorded
elsewhere:

```sh
bin/turgon audit verify --database-url "$RESTORED_DATABASE_URL"
```

For a Postgres you run yourself, `scripts/restore-drill.sh SOURCE_URL SCRATCH_URL` rehearses a
logical backup and restore. It checks that the restored audit chain has the same head, that
every table has the same row count, and that an entry edited in the copy is caught. CI runs it.

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

### The Turgon operator

With `operator.enabled`, the chart installs `turgon-operator` and the `Integration` CRD
(`turgon.dev/v1alpha1`, short name `tint`). Each spec in `specs` becomes an Integration. More
can be applied with kubectl, e.g. from a CI pipeline that compiles and signs them:

```sh
bin/turgon compile -c catalog shop-orders-to-erp -o shop.json --sign-key signing.key
bin/turgon integration -s shop.json --replicas 3 | kubectl apply -n turgon -f -
kubectl get tint -n turgon
# NAME                 SPEC                 LEVEL   READY   VALID   AVAILABLE
# shop-orders-to-erp   shop-orders-to-erp   L1      3       True    True
```

- **A spec is checked before it is rolled out.**
  - The operator parses the spec strictly: a field the schema doesn't know is refused.
  - It checks the digest and, with `specSigning.trustedKeys`, the signature.
  - A spec that fails any of these is not rolled out. The Integration reports
    `SpecValid=False` with the reason, and a warning event is recorded.
  - The workers keep running the last valid spec, so a tampered or unsigned spec never
    replaces a working one.
- **Versions are immutable.** Each spec version is its own immutable ConfigMap
  (`<name>-spec-<digest>`). Pods of the old version keep their spec until the rolling update
  replaces them. Old versions are deleted once every worker of the new one is ready.
- **Derived from the spec.**
  - The webhook Service and Ingress route exactly the paths of the spec's webhook events.
    They are removed when a new spec has none.
  - A PrometheusRule per Integration carries its latency SLO alerts
    (`monitoring.rules.enabled`).
- **Workers** come from a pod template the chart renders (image, database, Temporal,
  secrets, security settings). The operator adds the spec, arguments, ports and probes.
  `spec.replicas`, `spec.paused` (scale to zero; events wait at their sources),
  `spec.resources` and `spec.webhooks` are set per Integration. A large spec can be read from
  a ConfigMap with `spec.specFrom`; editing that ConfigMap rolls it out.
- **Status** records the running spec's name, digest, level and signing key, its workflows,
  ready workers, webhook paths, and the `SpecValid` and `Available` conditions.
- **Permissions and availability.** The operator acts only in its namespace, with a Role
  limited to what it manages. Several replicas elect a leader.

`make envtest` fetches a kube-apiserver and etcd. With `KUBEBUILDER_ASSETS` pointing at them,
`go test ./pkg/operator` runs the operator against a real API server.

### Appliance: one host, no Kubernetes

For sites without Kubernetes, and for sites with no internet access at all, releases include
an offline bundle, `turgon-appliance-<version>-linux-<arch>.tar.gz`. It contains:

- `turgon`;
- a single-node Temporal server: the Temporal CLI 1.4.1, built from source, persisted in SQLite;
- sandboxed systemd units;
- an installer that checks every file against `SHA256SUMS` before installing.

`make appliance` builds the bundle locally; see [`deploy/appliance/README.md`](deploy/appliance/README.md).

```sh
sudo ./install.sh --with-postgres          # PostgreSQL from the distribution, or your own
sudo install -o turgon -m 0640 shop.json /var/lib/turgon/specs/shop.json
/opt/turgon/bin/turgon appliance status
# NAME  STATE    DIGEST        LEVEL  PID    RESTARTS  NOTE
# shop  running  d4a93c4cfa21  L1     27553  0         b52bfeed2109 never became ready: ... set TURGON_SECRET_SALESFORCE_PROD_JWT
```

`turgon appliance run` supervises one worker per spec in `/var/lib/turgon/specs`, much as the
operator does for Integrations:

- **Checks before anything runs.** Each spec is parsed strictly and its digest is checked;
  with `TURGON_TRUSTED_KEYS` set, its signature too. A spec that fails is refused and the last
  valid version keeps running.
- **Safe version switches.** A new version starts next to the old one, which stops only when
  the new one is ready. A version that doesn't become ready is given up, with its reason
  shown by `status`, and the old one keeps serving.
- **Workers are restarted** with backoff when they exit.
- **Specs are immutable while they run.** Each version runs from an immutable copy of its
  spec.
- **`turgon appliance status` exits non-zero** while anything needs attention, so a
  monitoring check can call it.

### Governance: signed specs, mapping review, trained matching

**Signed runtime specs.** A compiled spec carries everything a worker enforces, including the
recipe's OPA policy packs. Its digest only catches accidental changes: anyone who can edit the
ConfigMap can loosen a policy and recompute the digest.
- The pipeline that compiles specs signs them with an Ed25519 key. The signature covers the
  name, the certification level and the digest.
- Workers, MCP servers and the gateway config load only specs signed by a trusted key. Changing a
  policy, or relabelling a spec's level, therefore has to go through that pipeline.
- A spec may carry several signatures, for key rotation.

```sh
bin/turgon keygen --out pipeline                        # pipeline.key (secret store), pipeline.pub
bin/turgon compile -c examples shop-orders-to-erp -o shop.json --sign-key pipeline.key
bin/turgon --trusted-keys pipeline.pub run -s shop.json  # or TURGON_TRUSTED_KEYS; unsigned specs are refused
helm upgrade turgon deploy/helm/turgon --reuse-values --set-file specSigning.trustedKeys=pipeline.pub
```

**Mapping review in the console.** Mapped fields below the confidence threshold wait in the
catalog's review queue.
- A data steward approves or rejects each one on the Catalog page. A rejection needs a reason, so
  the mapping's author can fix it.
- A decision holds for one expression of one mapping version: changing the expression puts the
  field back in the queue.
- Decisions are stored in Turgon's database and audited under the steward's name. Only fields
  actually in the queue can be reviewed.
- The console's reports apply the decisions. So does the compile pipeline, with
  `turgon compile --reviews-db "$TURGON_DATABASE_URL"`: an approved field no longer blocks its
  recipe, and a rejected one fails it with the steward's reason. The `approved: true` flag in
  mapping files remains the GitOps alternative.

**Identity matching trained per deployment.** Matching uses a Fellegi-Sunter model. How much
"same company email domain" or "same name" says about two records depends on the data. For
example, a marketplace that masks buyers behind one relay domain makes a shared domain nearly
meaningless.

`turgon identity train` estimates each comparison's weights from the deployment's own links:
- records linked to one master record are matches, and records of different ones are not;
- where data is thin, the estimates lean on the defaults;
- both models are evaluated on held-out master records, and the trained one is stored only if it
  is at least as precise.

Resolve steps then use it, and the training is audited.

```sh
bin/turgon identity train --entity Customer --threshold 0.95 --dry-run   # report only
bin/turgon identity train --entity Customer --threshold 0.95             # store for workers
```

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
| `pkg/operator`, `cmd/turgon-operator`, `apis/operator` | §11 | The Kubernetes operator: the Integration CRD and its reconciler |
| `pkg/appliance`, `deploy/appliance` | §11 | The single-host appliance: `turgon appliance`, systemd units, the offline bundle's installer |
| `pkg/connector` | §7.1 | Runtime connector interfaces, registry, secret resolution; `postgres/` is the native Postgres connector (outbox and change capture events, rollback dry-runs, idempotent writes); `salesforce/` reads by SOQL and the Bulk API 2.0, and subscribes over the Pub/Sub API (`pubsub/`: the gRPC wire protocol); `debezium/` consumes Debezium change events from Kafka |
| `pkg/secrets` | §9 | Secret references resolved from OpenBao or HashiCorp Vault (KV v2), signing in with Kubernetes service accounts, AppRoles or tokens; or from AWS Secrets Manager, Azure Key Vault and Google Secret Manager with the platform's workload identity |
| `pkg/connector/rest` | §7.1 | Generic HTTP JSON API connector configured per connection: cursor-polled list or search events (ascending, or newest-first paged back to the cursor), also received as signed webhooks (Stripe, Shopify, generic HMAC), reads, templated JSON or form-encoded writes, captured updates with preview, confirmation and restore; bearer, API-key header, basic and OAuth 2.0 client-credentials auth; `shoptest/`, `stripetest/` and `hubspottest/` fake the Shopify Admin, Stripe and HubSpot CRM APIs |
| `pkg/connector/remote`, `proto/` | §7.1 | The connector protocol (gRPC) and its Go client: connectors running in a sidecar, configured per endpoint with resolved secrets, reconfigured after a sidecar restart |
| `connectors/` | §7.1 | Camel/Java connectors: `sdk` (the protocol's server, Camel routes per operation), `sap-ecc` (BAPIs over RFC through SAP JCo), `sap-ecc-fake` (an in-memory ECC) |
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
| `pkg/plugin` | §18.4 | Logic plugin host: wazero sandbox speaking the Canonical ABI of `turgon:stack/logic-plugin` 0.1.0 and 0.2.0; `runner/` enforces grants, reads through the write guard, proposes governed writes, makes HTTP requests with host-inserted secrets, delivers events between plugins and audits each invocation |
| `pkg/netguard` | §9 | Keeps requests Turgon makes for others (A2A push notifications, plugins' HTTP) on public addresses |
| `wit/turgon-stack.wit` | §18.4 | Host interface for Wasm plugins (0.2.0; `wit/0.1.0/` keeps the first version) |
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

In rough roadmap order (§16, §19): Salesforce managed subscriptions (the Pub/Sub API keeping the replay position);
the metadata graph and discovery. Of the Camel/Java connectors, only SAP ECC exists so far. It is
tested against a fake ECC and a JCo stand-in, not yet against a real SAP system or gateway. It reads
IDocs as SAP pushes them, but not change pointers. The `shopify` and `powerbi-export` manifests
still have no runtime.
