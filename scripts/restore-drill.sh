#!/usr/bin/env bash
# restore-drill.sh SOURCE_URL SCRATCH_URL: rehearses restoring Turgon's state
# database from a logical backup, as a runbook would, and proves the copy
# is whole:
#
#   1. pg_dump SOURCE (custom format) and pg_restore it into SCRATCH,
#      a database this script may drop and recreate;
#   2. the audit log's hash chain verifies in the copy, with the same head
#      as the source;
#   3. every Turgon table holds as many rows as in the source;
#   4. negative control: an entry edited in the copy is caught.
#
# CloudNativePG deployments restore with the chart instead
# (database.cloudNativePG.recovery); the checks after a restore are the
# same. Needs pg_dump, pg_restore, psql and bin/turgon (make build).
set -euo pipefail

src=${1:?usage: restore-drill.sh SOURCE_URL SCRATCH_URL}
dst=${2:?usage: restore-drill.sh SOURCE_URL SCRATCH_URL}
turgon=${TURGON:-bin/turgon}
dump=$(mktemp -t turgon-state.XXXXXX.dump)
trap 'rm -f "$dump"' EXIT

scratch=${dst##*/}; scratch=${scratch%%\?*}
admin=${dst%/*}/postgres${dst#*"$scratch"}

echo "== backup"
pg_dump --format=custom --no-owner --file="$dump" "$src"
echo "dump: $(wc -c < "$dump") bytes"

echo "== restore into $scratch"
psql -q "$admin" -c "DROP DATABASE IF EXISTS \"$scratch\"" -c "CREATE DATABASE \"$scratch\""
pg_restore --no-owner --exit-on-error --dbname="$dst" "$dump"

echo "== audit chain"
before=$("$turgon" audit verify --database-url "$src")
after=$("$turgon" audit verify --database-url "$dst")
echo "source:   $before"
echo "restored: $after"
if [ "$before" != "$after" ]; then
  echo "FAIL: the restored audit log's head differs from the source's" >&2
  exit 1
fi

echo "== tables"
for t in turgon_audit turgon_writes turgon_cursors turgon_xref turgon_inbox; do
  a=$(psql -tAq "$src" -c "SELECT count(*) FROM $t" 2>/dev/null || echo absent)
  b=$(psql -tAq "$dst" -c "SELECT count(*) FROM $t" 2>/dev/null || echo absent)
  printf '%-16s source %-8s restored %s\n' "$t" "$a" "$b"
  if [ "$a" != "$b" ]; then
    echo "FAIL: $t differs" >&2
    exit 1
  fi
done

echo "== negative control: tamper with the copy"
psql -q "$dst" -c "ALTER TABLE turgon_audit DISABLE TRIGGER turgon_audit_no_change" \
  -c "UPDATE turgon_audit SET line = replace(line, '\"actor\":\"', '\"actor\":\"x') WHERE seq = (SELECT min(seq) FROM turgon_audit)"
if "$turgon" audit verify --database-url "$dst" >/dev/null 2>&1; then
  echo "FAIL: an edited audit entry went unnoticed" >&2
  exit 1
fi
echo "tampering detected"
psql -q "$admin" -c "DROP DATABASE \"$scratch\""
echo "restore drill passed"
