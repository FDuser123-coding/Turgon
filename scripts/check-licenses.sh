#!/usr/bin/env bash
# License policy for anything shipped to customers (architecture §12):
# allowed are Apache-2.0, MIT, BSD, ISC and MPL-2.0. Anything else (LGPL and
# EPL need a case-by-case review; AGPL, SSPL, BSL and the Elastic License are
# never allowed) fails unless listed in scripts/license-exceptions.txt as
# "<module or package> <license> <who reviewed it and why>".
set -euo pipefail
cd "$(dirname "$0")/.."

allowed='^(Apache-2\.0|MIT|BSD-2-Clause|BSD-3-Clause|ISC|MPL-2\.0)$'
exceptions=scripts/license-exceptions.txt
fail=0

check() { # source name license
  local src=$1 name=$2 license=$3
  if [[ $license =~ $allowed ]]; then return; fi
  if [[ -f $exceptions ]] && grep -qE "^${name//./\\.} ${license//./\\.} " "$exceptions"; then
    echo "reviewed  $src $name $license"
    return
  fi
  echo "NOT ALLOWED  $src $name: $license"
  fail=1
}

# Go modules compiled into porter (our own module is excluded).
command -v go-licenses >/dev/null || { echo "install: go install github.com/google/go-licenses/v2@latest"; exit 2; }
while IFS=, read -r module _ license; do
  check go "$module" "$license"
done < <(go-licenses report ./... --ignore github.com/fduser123-coding/turgon 2>/dev/null)

# npm packages bundled into the console (production dependencies only).
(cd console && npx --yes license-checker@25 --production --excludePrivatePackages --csv) |
  tail -n +2 | tr -d '"' |
  while IFS=, read -r pkg license _; do
    check npm "${pkg%@*}" "$license" || true
  done > /tmp/npm-license-check.txt
cat /tmp/npm-license-check.txt
grep -q '^NOT ALLOWED' /tmp/npm-license-check.txt && fail=1

if [[ $fail -ne 0 ]]; then
  echo "license check failed"
  exit 1
fi
echo "license check passed"
