.PHONY: all build console test test-integration vet fmt demo spec chart image licenses

all: fmt vet test console build

build:
	go build -o bin/turgon ./cmd/turgon

# Build the web console into pkg/console/dist, which `build` embeds.
console:
	cd console && npm ci && npm test && npm run build

# Unit tests; Postgres integration tests skip without TURGON_TEST_DATABASE_URL.
test:
	go test -race ./...

# Requires a Postgres, e.g. TURGON_TEST_DATABASE_URL=postgres://turgon@localhost:5432/turgon_test
test-integration:
	@test -n "$$TURGON_TEST_DATABASE_URL" || (echo "set TURGON_TEST_DATABASE_URL" && exit 1)
	go test -race -count=1 ./pkg/store/... ./pkg/connector/... ./pkg/engine/...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Verify and compile the §18.5 example stack.
demo:
	go run ./cmd/turgon verify -c examples eu-distributor-core
	go run ./cmd/turgon compile -c examples eu-distributor-core

# Compile the prototype's end-to-end flow for `turgon run`.
spec: build
	bin/turgon compile -c examples shop-orders-to-erp -o runtime-spec.json

# Render the Helm chart with the example specs (needs helm).
chart: build
	bin/turgon compile -c examples shop-orders-to-erp -o /tmp/turgon-shop.json
	helm lint deploy/helm/turgon --set database.existingSecret=turgon-db \
		--set console.auth.trustedProxies={10.0.0.0/8} --set console.auth.approverGroup=turgon-approvers \
		--set-file specs.shop-orders-to-erp=/tmp/turgon-shop.json

# Build the container image (needs a container builder).
image:
	docker build -t ghcr.io/fduser123-coding/turgon:dev .

# Check shipped dependencies against the license policy (needs go-licenses).
licenses:
	scripts/check-licenses.sh
