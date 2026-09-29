# Turgon appliance

Turgon on one Linux host (x86-64 or arm64, systemd), without Kubernetes, air-gapped if need be.
This bundle holds everything it runs:

| | |
|---|---|
| `bin/turgon` | Turgon: the workers, the console, and `turgon appliance run`, which supervises them |
| `bin/temporal` | A single-node Temporal server (the Temporal CLI's), persisted in SQLite |
| `systemd/` | `turgon-appliance`, `turgon-console` and `turgon-temporal` units, sandboxed |
| `install.sh` | Checks `SHA256SUMS`, installs, and optionally sets up PostgreSQL |

```sh
tar xzf turgon-appliance-<version>-linux-amd64.tar.gz && cd turgon-appliance-*
sudo ./install.sh --with-postgres        # or set TURGON_DATABASE_URL to your PostgreSQL
sudoedit /etc/turgon/turgon.env           # connection secrets, TURGON_TRUSTED_KEYS
sudo install -o turgon -m 0640 shop.json /var/lib/turgon/specs/shop.json
/opt/turgon/bin/turgon appliance status
```

`turgon appliance run` treats `/var/lib/turgon/specs` as the operator treats Integrations:

- **Every spec is checked before it runs.** It is parsed strictly and its digest is checked;
  with `TURGON_TRUSTED_KEYS` set, its signature is checked too. A spec that fails is refused,
  with the reason shown by `turgon appliance status`, and the worker of its last valid version
  keeps running.
- **Replacing a spec switches versions safely.** The new version's worker starts next to the
  old one, and the old worker stops only once the new one answers `/readyz`. A new version that
  is not ready within `--ready-timeout` (2 minutes) is given up, for example when a connection
  secret is missing. The old version keeps running, and the new one is not retried until the
  file changes again.
- **Workers are restarted.** A worker that exits is restarted with backoff, and deleting a
  spec stops its worker.
- **Running specs can't change under a worker.** Each version runs from an immutable copy in
  `/var/lib/turgon/state`, so editing the directory never changes what a running worker reads.

Each worker serves `/readyz` and `/metrics` on a loopback port from 18100 up;
`turgon appliance status --json` lists them.

The Temporal server is meant for one host. For high availability, point
`TURGON_TEMPORAL_ADDRESS` at a Temporal cluster or Temporal Cloud and disable `turgon-temporal`.
Back up the PostgreSQL database (and `/var/lib/turgon/temporal` for runs in flight) as you back
up your other databases. `scripts/restore-drill.sh` in the source tree shows the procedure.
