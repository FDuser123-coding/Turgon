.PHONY: all build test vet fmt demo

all: fmt vet test build

build:
	go build -o bin/porter ./cmd/porter

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Verify and compile the §18.5 example stack.
demo:
	go run ./cmd/porter verify -c examples eu-distributor-core
	go run ./cmd/porter compile -c examples eu-distributor-core
