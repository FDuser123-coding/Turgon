.PHONY: all build console test test-integration vet fmt demo spec chart image

all: fmt vet test console build

build:
	go build -o bin/porter ./cmd/porter

# Build the web console into pkg/console/dist, which `build` embeds.
console:
	cd console && npm ci && npm test && npm run build

# Unit tests; Postgres integration tests skip without PORTER_TEST_DATABASE_URL.
test:
	go test -race ./...

# Requires a Postgres, e.g. PORTER_TEST_DATABASE_URL=postgres://porter@localhost:5432/porter_test
test-integration:
	@test -n "$$PORTER_TEST_DATABASE_URL" || (echo "set PORTER_TEST_DATABASE_URL" && exit 1)
	go test -race -count=1 ./pkg/store/... ./pkg/connector/... ./pkg/engine/...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Verify and compile the §18.5 example stack.
demo:
	go run ./cmd/porter verify -c examples eu-distributor-core
	go run ./cmd/porter compile -c examples eu-distributor-core

# Compile the prototype's end-to-end flow for `porter run`.
spec: build
	bin/porter compile -c examples shop-orders-to-erp -o runtime-spec.json

# Render the Helm chart with the example specs (needs helm).
chart: build
	bin/porter compile -c examples shop-orders-to-erp -o /tmp/porter-shop.json
	helm lint deploy/helm/porter --set database.existingSecret=porter-db \
		--set console.auth.trustedProxies={10.0.0.0/8} --set console.auth.approverGroup=porter-approvers \
		--set-file specs.shop-orders-to-erp=/tmp/porter-shop.json

# Build the container image (needs a container builder).
image:
	docker build -t ghcr.io/fduser123-coding/porter:dev .
