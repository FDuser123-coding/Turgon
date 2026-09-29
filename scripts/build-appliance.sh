#!/usr/bin/env bash
# Builds the offline appliance bundle:
#   dist/turgon-appliance-<version>-linux-<arch>.tar.gz
# with turgon (console embedded if built), the Temporal CLI built from
# source at a pinned version, the systemd units, install.sh and SHA256SUMS.
#
#   scripts/build-appliance.sh [version] [arch]
set -euo pipefail
cd "$(dirname "$0")/.."

version=${1:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}
arch=${2:-amd64}
temporal_version=v1.4.1
name=turgon-appliance-$version-linux-$arch
out=dist/$name
rm -rf "$out" && mkdir -p "$out/bin" "$out/systemd"

export CGO_ENABLED=0 GOOS=linux GOARCH=$arch
go build -trimpath -ldflags "-s -w -X main.version=$version" -o "$out/bin/turgon" ./cmd/turgon
# The Temporal CLI, from source at a pinned version (a throwaway module, so
# it builds for any architecture and never touches turgon's go.mod).
tmp=$(mktemp -d) && trap 'rm -rf "$tmp"' EXIT
(cd "$tmp" && go mod init appliance-temporal >/dev/null 2>&1 && go get "github.com/temporalio/cli/cmd/temporal@$temporal_version" >/dev/null 2>&1 \
  && go build -trimpath -ldflags "-s -w -X github.com/temporalio/cli/temporalcli.Version=${temporal_version#v}" \
     -o "$OLDPWD/$out/bin/temporal" github.com/temporalio/cli/cmd/temporal \
  && mkdir -p "$OLDPWD/$out/licenses" \
  && cp "$(go list -m -f '{{.Dir}}' github.com/temporalio/cli)/LICENSE" "$OLDPWD/$out/licenses/temporal-cli.txt")
test -x "$out/bin/temporal" || { echo "building the Temporal CLI failed" >&2; exit 1; }

cp deploy/appliance/turgon-*.service "$out/systemd/"
cp deploy/appliance/install.sh deploy/appliance/turgon.env.example deploy/appliance/README.md "$out/"
echo "$version" > "$out/VERSION"
echo "temporal $temporal_version" >> "$out/VERSION"
(cd "$out" && find . -type f ! -name SHA256SUMS -printf '%P\n' | sort | xargs sha256sum > SHA256SUMS)
tar -C dist -czf "dist/$name.tar.gz" "$name"
rm -rf "$out"
(cd dist && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
echo "dist/$name.tar.gz"
