#!/usr/bin/env bash
# Installs the Turgon appliance from this bundle: turgon and the Temporal
# CLI in /opt/turgon/bin, systemd units, /etc/turgon/turgon.env, and
# /var/lib/turgon for specs and state. Works offline. Run as root:
#
#   sudo ./install.sh [--with-postgres] [--no-start]
#
# --with-postgres installs PostgreSQL from the distribution's packages and
#   creates Turgon's database and user; without it, set TURGON_DATABASE_URL
#   in /etc/turgon/turgon.env to an existing database.
# --root DIR installs into DIR instead of / (for image builds and tests):
#   no user is created, no service is started.
set -euo pipefail

root="" with_postgres=0 start=1
while [[ $# -gt 0 ]]; do
  case $1 in
    --root) root=${2%/}; shift 2 ;;
    --with-postgres) with_postgres=1; shift ;;
    --no-start) start=0; shift ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

if [[ -z $root && $(id -u) -ne 0 ]]; then
  echo "run as root (or with --root DIR)" >&2; exit 1
fi

# The bundle is what was built: every file matches its checksum.
if ! sha256sum --quiet -c SHA256SUMS; then
  echo "the bundle does not match SHA256SUMS; refusing to install" >&2; exit 1
fi

owner=turgon
if [[ -z $root ]]; then
  if ! id -u turgon >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/turgon --shell /usr/sbin/nologin turgon
  fi
else
  owner=$(id -un)
fi

install -d -m 0755 "$root/opt/turgon/bin" "$root/etc/systemd/system"
install -d -m 0750 "$root/etc/turgon"
install -m 0755 bin/turgon bin/temporal "$root/opt/turgon/bin/"
install -m 0644 systemd/turgon-appliance.service systemd/turgon-console.service systemd/turgon-temporal.service "$root/etc/systemd/system/"
install -d -m 0750 -o "$owner" "$root/var/lib/turgon" "$root/var/lib/turgon/specs" "$root/var/lib/turgon/state" "$root/var/lib/turgon/temporal"

env_file="$root/etc/turgon/turgon.env"
if [[ -e $env_file ]]; then
  echo "keeping the existing $env_file"
else
  install -m 0640 turgon.env.example "$env_file"
  if [[ -z $root ]]; then chgrp turgon "$env_file" "$root/etc/turgon"; fi
fi

if [[ $with_postgres -eq 1 && -z $root ]]; then
  if ! command -v psql >/dev/null; then
    if command -v apt-get >/dev/null; then apt-get install -y postgresql
    elif command -v dnf >/dev/null; then dnf install -y postgresql-server && postgresql-setup --initdb
    else echo "install PostgreSQL 14 or later, then run again" >&2; exit 1; fi
  fi
  systemctl enable --now postgresql
  password=$(head -c 24 /dev/urandom | base64 | tr -d '/+=')
  if ! runuser -u postgres -- psql -tAc "SELECT 1 FROM pg_roles WHERE rolname = 'turgon'" | grep -q 1; then
    runuser -u postgres -- psql -q -c "CREATE ROLE turgon LOGIN PASSWORD '$password'"
    runuser -u postgres -- psql -q -c "CREATE DATABASE turgon OWNER turgon"
    sed -i "s|^TURGON_DATABASE_URL=.*|TURGON_DATABASE_URL=postgres://turgon:$password@127.0.0.1:5432/turgon?sslmode=disable|" "$env_file"
    echo "created database turgon; its URL is in $env_file"
  fi
fi

if [[ -z $root ]]; then
  systemctl daemon-reload
  if [[ $start -eq 1 ]]; then
    systemctl enable --now turgon-temporal.service turgon-appliance.service turgon-console.service
  fi
fi

cat <<EOF
Installed Turgon $(cat VERSION) in ${root:-/}.
Next:
  1. Edit /etc/turgon/turgon.env: the database, connection secrets (turgon secrets <spec>),
     and TURGON_TRUSTED_KEYS to run only signed specs.
  2. Put compiled specs in /var/lib/turgon/specs/<name>.json (owned by turgon).
  3. /opt/turgon/bin/turgon appliance status
EOF
