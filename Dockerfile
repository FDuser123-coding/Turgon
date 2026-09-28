# syntax=docker/dockerfile:1.7
# Turgon worker and console. Multi-stage: the web console is built with
# Node, the binary with Go (static, no cgo), and the result runs on a
# distroless base as a non-root user with a read-only root filesystem.

FROM node:22-bookworm-slim AS console
WORKDIR /src/console
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY console/ ./
COPY pkg/console/dist/robots.txt /src/pkg/console/dist/robots.txt
RUN npm test && npm run build

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=console /src/pkg/console/dist/ pkg/console/dist/
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/turgon ./cmd/turgon

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/turgon /turgon
USER 65532:65532
ENTRYPOINT ["/turgon"]
