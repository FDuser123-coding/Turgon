#!/bin/sh
# Rebuilds the Wasm plugins committed to the repository from their sources:
# the credit-check example and the probe the host tests run. With the
# pinned toolchain (rust-toolchain.toml) and paths remapped, the output is
# the same on any machine; CI checks the committed files match.
#   scripts/build-plugins.sh            rebuild in place
#   scripts/build-plugins.sh --check    fail if a committed module differs
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
check=${1:-}
cargo_home=${CARGO_HOME:-$HOME/.cargo}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

build() { # crate dir, target, artifact name, destination
	(
		cd "$root/$1"
		RUSTFLAGS="--remap-path-prefix=$cargo_home/registry/src=/crates --remap-path-prefix=$root=/turgon" \
			cargo build --quiet --locked --release --target "$2"
	)
	cp "$root/$1/target/$2/release/$3" "$out/$(basename "$4")"
	place "$4"
}

place() { # destination
	if [ "$check" = "--check" ]; then
		if ! cmp -s "$out/$(basename "$1")" "$root/$1"; then
			echo "$1 differs from what its sources build; run scripts/build-plugins.sh" >&2
			exit 1
		fi
	else
		cp "$out/$(basename "$1")" "$root/$1"
	fi
}

build examples/plugins/credit-check wasm32-unknown-unknown credit_check.wasm examples/plugins/credit-check/credit-check.wasm
build pkg/plugin/testdata/probe wasm32-unknown-unknown probe.wasm pkg/plugin/testdata/probe.wasm
build pkg/plugin/testdata/probe wasm32-wasip1 probe.wasm pkg/plugin/testdata/probe-wasi.wasm
wasm-tools component new "$out/probe.wasm" -o "$out/probe.component.wasm"
place pkg/plugin/testdata/probe.component.wasm
if [ "$check" = "--check" ]; then echo "plugins match their sources"; else echo "plugins built"; fi
