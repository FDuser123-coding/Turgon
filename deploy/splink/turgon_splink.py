"""Turgon's Splink service: probabilistic record linkage for resolve steps
that use ``strategy: splink`` (architecture §7.3).

It reads the links Turgon has already confirmed (the ``turgon_xref`` table:
each source record, the master record it belongs to, and its normalized
identifying attributes), trains a Splink model per entity on them, and
answers, for a new record, which master records it may be and how likely.
Turgon decides what to link: automatically above the recipe's threshold,
otherwise through a data steward, whose links train the next model.

    POST /v1/match  {"entity": "Customer", "attributes": {...}, "limit": 3}
    POST /v1/train  retrain now, from the current links
    GET  /healthz   the models, without any record data

Settings (environment):
    TURGON_SPLINK_DATABASE_URL  Postgres URL of Turgon's state (read access
                                to turgon_xref is enough)
    TURGON_SPLINK_TOKEN         if set, requests need "Authorization: Bearer <token>"
    TURGON_SPLINK_LISTEN        address to listen on, default 0.0.0.0:8080
    TURGON_SPLINK_RETRAIN       seconds between checks for new links, default 300
    TURGON_SPLINK_PRIOR         prior probability that a record and a candidate
                                sharing a blocking key are the same entity,
                                default 0.05 (as Turgon's built-in model)

Attributes are personal data: the service logs counts, never values.
"""

from __future__ import annotations

import hmac
import json
import logging
import math
import os
import re
import threading
import time
import warnings
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterable

import pandas as pd
import splink.comparison_level_library as cll
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

log = logging.getLogger("turgon-splink")
logging.getLogger("splink").setLevel(logging.ERROR)

# Starting m and u probabilities, the same as Turgon's built-in model
# (pkg/identity.Default); training replaces those the links can estimate.
# Levels are listed best first, without the null level.
EXACT_M, EXACT_U = [0.95, 0.05], [0.001, 0.999]
# contact: same email, else same company domain, else neither. As in
# pkg/identity, the domain is evidence of its own only when the addresses
# differ, so the two are one comparison rather than two that double count.
CONTACT_M, CONTACT_U = [0.80, 0.15, 0.05], [0.0001, 0.002, 0.9979]
# name: exact, Jaro-Winkler >= 0.94, >= 0.84, else (as pkg/identity).
NAME_M, NAME_U = [0.80, 0.05, 0.10, 0.05], [0.01, 0.01, 0.05, 0.93]

REASONS = {
    "contact": {2: "same email address", 1: "same company email domain"},
    "name": {3: "same name", 2: "same name", 1: "similar name"},
}

_COLUMN = re.compile(r"[^a-z0-9_]")


def column_for(key: str) -> str | None:
    """The DuckDB column for an attribute key: email, domain, name, or
    exact:<field> -> exact_<field>."""
    if key in ("email", "domain", "name"):
        return key
    if key.startswith("exact:"):
        return "exact_" + _COLUMN.sub("_", key[len("exact:"):].lower())
    return None


@dataclass
class Link:
    system: str
    source_id: str
    master: str
    attributes: dict


@dataclass
class EntityModel:
    """A trained model for one entity, and the records it matches against."""

    entity: str
    linker: Linker
    columns: list[str]
    masters: dict[str, str]  # unique_id -> master
    records: int
    master_count: int
    prior: float
    trained: list[str]
    trained_at: float
    digest: str
    fields: dict[str, str] = field(default_factory=dict)  # exact_<f> column -> field name
    lock: threading.Lock = field(default_factory=threading.Lock)

    def summary(self) -> dict:
        return {
            "records": self.records,
            "masters": self.master_count,
            "prior": round(self.prior, 6),
            "trained": self.trained,
            "trainedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.trained_at)),
        }


def _contact():
    return cl.CustomComparison(
        output_column_name="contact",
        comparison_levels=[
            cll.CustomLevel("(email_l IS NULL OR email_r IS NULL) AND (domain_l IS NULL OR domain_r IS NULL)",
                            "no address to compare").configure(is_null_level=True),
            cll.CustomLevel("email_l = email_r", "same email"),
            cll.CustomLevel("domain_l = domain_r", "same domain"),
            cll.ElseLevel(),
        ],
    ).configure(m_probabilities=CONTACT_M, u_probabilities=CONTACT_U)


def _comparisons(columns: Iterable[str]):
    """One comparison per kind of attribute; email and domain make one."""
    out = []
    columns = list(columns)
    if "email" in columns or "domain" in columns:
        out.append(_contact())
    for c in columns:
        if c in ("email", "domain"):
            continue
        if c == "name":
            out.append(cl.JaroWinklerAtThresholds("name", [0.94, 0.84]).configure(m_probabilities=NAME_M, u_probabilities=NAME_U))
        else:
            out.append(cl.ExactMatch(c).configure(m_probabilities=EXACT_M, u_probabilities=EXACT_U))
    return out


def _blocking(columns: Iterable[str]):
    rules = []
    for c in columns:
        rules.append(block_on("substr(name, 1, 3)") if c == "name" else block_on(c))
    return rules


# Strength is how many pairs' worth of weight the starting values keep
# against each estimate, so a level seen a few times cannot swing to an
# extreme (as pkg/identity.Train).
STRENGTH = 20.0
MAX_U_PAIRS = 1_000_000


def _clamp(p: float) -> float:
    return min(max(p, 1e-6), 1 - 1e-6)


def smooth(start: dict, trained: dict, m_pairs: int, u_pairs: int) -> dict:
    """The trained settings with each m and u drawn towards its starting
    value: (pairs * estimate + strength * start) / (pairs + strength)."""
    for c0, c1 in zip(start["comparisons"], trained["comparisons"]):
        for l0, l1 in zip(c0["comparison_levels"], c1["comparison_levels"]):
            if l1.get("is_null_level"):
                continue
            for key, n in (("m_probability", m_pairs), ("u_probability", u_pairs)):
                d, t = l0.get(key), l1.get(key)
                if d is None:
                    continue
                if t is None or n == 0:
                    l1[key] = d
                else:
                    l1[key] = _clamp((n * t + STRENGTH * d) / (n + STRENGTH))
    return trained


def _linker(df, settings) -> Linker:
    # Splink would log each training step at INFO to stdout otherwise.
    return Linker(df, settings, db_api=DuckDBAPI(), set_up_basic_logging=False)


def train(entity: str, links: list[Link], digest: str = "", prior: float = 0.05) -> EntityModel | None:
    """Builds and trains a model from an entity's links; None without links."""
    if not links:
        return None
    columns = sorted({c for l in links for k in l.attributes for c in [column_for(k)] if c})
    if not columns:
        return None
    if "email" in columns or "domain" in columns:  # the contact comparison reads both
        columns = sorted(set(columns) | {"email", "domain"})
    rows, masters = [], {}
    for l in links:
        uid = f"{l.system}/{l.source_id}"
        row = {"unique_id": uid, "master_id": l.master}
        for k, v in l.attributes.items():
            c = column_for(k)
            if c:
                row[c] = v
        rows.append(row)
        masters[uid] = l.master
    df = pd.DataFrame(rows)
    for c in columns:
        if c not in df:
            df[c] = None
    per_master: dict[str, int] = {}
    for m in masters.values():
        per_master[m] = per_master.get(m, 0) + 1
    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=_comparisons(columns),
        blocking_rules_to_generate_predictions=_blocking(columns),
        probability_two_random_records_match=prior,
        retain_intermediate_calculation_columns=True,  # gamma_* columns: the reasons
        retain_matching_columns=True,
    )
    linker = _linker(df, settings)
    start = linker.misc.save_model_to_json()
    k = len(per_master)
    u_pairs = min(k * (k - 1) // 2, MAX_U_PAIRS)
    m_pairs = sum(n * (n - 1) // 2 for n in per_master.values())
    trained = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if m_pairs:
            linker.training.estimate_m_from_label_column("master_id")
            trained.append("m")
        if u_pairs:
            # u is how often a level occurs by chance between different
            # entities: sampled from one record per master, so pairs the
            # links say are the same entity do not count as chance.
            one = _linker(df.drop_duplicates("master_id"), settings)
            one.training.estimate_u_using_random_sampling(max_pairs=MAX_U_PAIRS, seed=1)
            u = one.misc.save_model_to_json()
            trained.append("u")
    if trained:
        out = linker.misc.save_model_to_json()
        if u_pairs:
            for c, cu in zip(out["comparisons"], u["comparisons"]):
                for l, lu in zip(c["comparison_levels"], cu["comparison_levels"]):
                    if "u_probability" in lu:
                        l["u_probability"] = lu["u_probability"]
        linker = _linker(df, smooth(start, out, m_pairs, u_pairs))
    fields = {column_for(k): k[len("exact:"):] for l in links for k in l.attributes if k.startswith("exact:")}
    return EntityModel(entity, linker, columns, masters, len(rows), len(per_master), prior, trained, time.time(), digest, fields)


def _gamma_columns(columns: list[str]) -> list[str]:
    out = ["contact"] if "email" in columns else []
    return out + [c for c in columns if c not in ("email", "domain")]


def match(model: EntityModel, attributes: dict, limit: int = 3) -> list[dict]:
    """The master records a record may be, best first: each master's
    best-matching linked record, with the evidence for it."""
    row = {"unique_id": "__new__"}
    for k, v in attributes.items():
        c = column_for(k)
        if c in model.columns and isinstance(v, str) and v:
            row[c] = v
    present = [c for c in model.columns if c in row]
    if not present:
        return []
    for c in model.columns:
        row.setdefault(c, None)
    new = pd.DataFrame([row])
    with model.lock, warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = model.linker.inference.find_matches_to_new_records(
            new, blocking_rules=_blocking(present), match_weight_threshold=-30
        ).as_pandas_dataframe()
    best: dict[str, dict] = {}
    for _, r in res.iterrows():
        other = r["unique_id_l"] if r["unique_id_r"] == "__new__" else r["unique_id_r"]
        master = model.masters.get(other)
        if master is None:
            continue
        p = float(r["match_probability"])
        if master in best and best[master]["probability"] >= p:
            continue
        reasons = []
        for c in _gamma_columns(model.columns):
            g = r.get(f"gamma_{c}")
            if g is None or (isinstance(g, float) and math.isnan(g)):
                continue
            text = REASONS[c].get(int(g)) if c in REASONS else ("same " + model.fields.get(c, c) if int(g) == 1 else None)
            if text:
                reasons.append(text)
        if not reasons:
            continue  # nothing in common: no suggestion (as pkg/identity)
        best[master] = {"master": master, "probability": round(p, 6), "matchWeight": round(float(r["match_weight"]), 4), "reasons": reasons}
    return sorted(best.values(), key=lambda s: (-s["probability"], s["master"]))[:limit]


class Models:
    """The trained models, retrained when the links change."""

    def __init__(self, load, prior: float = 0.05):
        self._load = load  # () -> dict[entity, (digest, list[Link])]
        self._prior = prior
        self._models: dict[str, EntityModel] = {}
        self._mu = threading.Lock()
        self.ready = False
        self.error = ""

    def refresh(self, force: bool = False) -> list[str]:
        """Retrains the entities whose links changed; returns their names."""
        changed = []
        loaded = self._load()
        for entity, (digest, links) in loaded.items():
            cur = self._models.get(entity)
            if not force and cur is not None and cur.digest == digest:
                continue
            try:
                m = train(entity, links, digest, self._prior)
            except Exception as e:  # noqa: BLE001 - keep this entity's last model, train the others
                log.warning("could not train %s: %s", entity, type(e).__name__)
                continue
            with self._mu:
                if m is None:
                    self._models.pop(entity, None)
                else:
                    self._models[entity] = m
            changed.append(entity)
            log.info("trained %s on %d records of %d masters (%s)", entity, len(links), m.master_count if m else 0, ",".join(m.trained) if m else "-")
        with self._mu:
            for gone in set(self._models) - set(loaded):
                del self._models[gone]
        self.ready, self.error = True, ""
        return changed

    def get(self, entity: str) -> EntityModel | None:
        with self._mu:
            return self._models.get(entity)

    def summary(self) -> dict:
        with self._mu:
            return {e: m.summary() for e, m in sorted(self._models.items())}


def postgres_loader(url: str):
    """Reads turgon_xref, grouped by entity, with a digest per entity that
    changes when any of its links does."""
    import psycopg

    def load():
        out: dict[str, tuple[str, list[Link]]] = {}
        with psycopg.connect(url, autocommit=True) as conn:
            digests = dict(conn.execute(
                "SELECT entity, md5(string_agg(system || '/' || source_id || '=' || master_id || attributes::text, ',' "
                "ORDER BY system, source_id)) FROM turgon_xref GROUP BY entity").fetchall())
            for entity, digest in digests.items():
                rows = conn.execute(
                    "SELECT system, source_id, master_id, attributes FROM turgon_xref WHERE entity = %s", (entity,)).fetchall()
                out[entity] = (digest, [Link(s, sid, m, a or {}) for s, sid, m, a in rows])
        return out

    return load


def handler(models: Models, token: str):
    class Handler(BaseHTTPRequestHandler):
        server_version = "turgon-splink"

        def log_message(self, fmt, *args):  # no request lines: paths are enough, bodies never
            log.debug("%s %s", self.command, self.path)

        def _send(self, status: int, body: dict):
            b = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def _authorized(self) -> bool:
            if not token:
                return True
            got = self.headers.get("Authorization", "")
            if hmac.compare_digest(got.encode(), ("Bearer " + token).encode()):
                return True
            self._send(401, {"error": "a bearer token is required"})
            return False

        def _body(self) -> dict | None:
            n = int(self.headers.get("Content-Length") or 0)
            if n > 1 << 20:
                self._send(413, {"error": "the request is larger than 1 MiB"})
                return None
            try:
                v = json.loads(self.rfile.read(n) or b"{}")
                return v if isinstance(v, dict) else None
            except ValueError:
                return None

        def do_GET(self):
            if self.path != "/healthz":
                return self._send(404, {"error": "not found"})
            if not models.ready:
                return self._send(503, {"status": "starting", "error": models.error})
            self._send(200, {"status": "ok", "entities": models.summary()})

        def do_POST(self):
            if not self._authorized():
                return
            if self.path == "/v1/train":
                try:
                    return self._send(200, {"retrained": models.refresh(force=True), "entities": models.summary()})
                except Exception as e:  # noqa: BLE001 - reported to the caller
                    return self._send(503, {"error": f"could not read the links: {type(e).__name__}"})
            if self.path != "/v1/match":
                return self._send(404, {"error": "not found"})
            req = self._body()
            if req is None or not isinstance(req.get("entity"), str) or not isinstance(req.get("attributes"), dict):
                return self._send(400, {"error": 'send {"entity": "...", "attributes": {...}}'})
            if not models.ready:
                return self._send(503, {"error": "the models are not trained yet"})
            m = models.get(req["entity"])
            if m is None:
                return self._send(404, {"error": "no links for " + req["entity"] + " yet: link records through the steward queue first"})
            limit = req.get("limit", 3)
            limit = limit if isinstance(limit, int) and 1 <= limit <= 20 else 3
            self._send(200, {"entity": m.entity, "model": m.summary(), "candidates": match(m, req["attributes"], limit)})

    return Handler


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = os.environ.get("TURGON_SPLINK_DATABASE_URL", "")
    if not url:
        raise SystemExit("TURGON_SPLINK_DATABASE_URL is required: the Postgres URL of Turgon's state")
    token = os.environ.get("TURGON_SPLINK_TOKEN", "")
    host, _, port = os.environ.get("TURGON_SPLINK_LISTEN", "0.0.0.0:8080").rpartition(":")
    every = int(os.environ.get("TURGON_SPLINK_RETRAIN", "300"))
    prior = float(os.environ.get("TURGON_SPLINK_PRIOR", "0.05"))
    if not 0 < prior < 1:
        raise SystemExit("TURGON_SPLINK_PRIOR must be between 0 and 1")
    models = Models(postgres_loader(url), prior)

    def loop():
        while True:
            try:
                models.refresh()
            except Exception as e:  # noqa: BLE001 - keep serving the last models
                models.error = type(e).__name__
                log.warning("could not read the links: %s", type(e).__name__)
            time.sleep(every)

    threading.Thread(target=loop, daemon=True).start()
    srv = ThreadingHTTPServer((host or "0.0.0.0", int(port)), handler(models, token))
    log.info("turgon-splink listening on %s:%s%s", host or "0.0.0.0", port, "" if token else " (no token: any caller may match)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
