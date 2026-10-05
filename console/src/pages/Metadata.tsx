import { useState } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Link, Pill } from "../components";
import { ago, breaks, flags, values, where } from "../format";
import { navigate, usePoll, useSearch } from "../hooks";
import type { MetaDetail as Detail, MetaObject, MetaSystem } from "../types";

export function Metadata() {
  const { data, error } = usePoll(api.meta, 30000);
  const systems = data?.systems ?? [];
  const breaking = systems.reduce((n, s) => n + s.breaking, 0);
  const missing = systems.reduce((n, s) => n + s.missing, 0);
  return (
    <section>
      <header className="page-head">
        <h1>Metadata</h1>
        <p className="muted">
          What each connected system holds, as <span className="mono">turgon discover</span> last found it: tables, sObjects,
          entity sets, BAPIs and IDoc segments, with their fields, and what in Turgon uses each field. A change that can break
          a run is flagged when something uses what changed.
        </p>
      </header>
      <ErrorBanner error={error ?? data?.error ?? null} />
      <div className="tiles">
        <div className="tile">
          <span className="tile-value">{systems.length}</span>
          <span className="tile-label">systems discovered</span>
        </div>
        <div className="tile">
          <span className={`tile-value${breaking > 0 ? " tone-bad" : ""}`}>{breaking}</span>
          <span className="tile-label">breaking changes in use</span>
        </div>
        <div className="tile">
          <span className={`tile-value${missing > 0 ? " tone-bad" : ""}`}>{missing}</span>
          <span className="tile-label">used but missing</span>
        </div>
      </div>
      {data && systems.length === 0 && !data.error && (
        <Empty>
          Nothing discovered yet. Run <span className="mono">turgon discover -s spec.json --database-url …</span>, on a schedule.
        </Empty>
      )}
      {systems.map((s) => (
        <SystemCard key={s.endpoint} system={s} />
      ))}
    </section>
  );
}

function SystemCard({ system: s }: { system: MetaSystem }) {
  return (
    <article className="card">
      <div className="card-head">
        <div>
          <Link to={`/metadata/${encodeURIComponent(s.endpoint)}`} className="title mono">
            {s.endpoint}
          </Link>{" "}
          <span className="muted small">
            {s.connector}
            {s.version ? ` ${s.version}` : ""}
          </span>
        </div>
        <div>
          {s.breaking > 0 && <Pill tone="bad">{s.breaking} breaking</Pill>}{" "}
          {s.missing > 0 && <Pill tone="bad">{s.missing} missing</Pill>}{" "}
          {s.changes - s.breaking > 0 && <Pill tone="info">{s.changes - s.breaking} other changes</Pill>}{" "}
          {s.changes === 0 && s.missing === 0 && <Pill tone="ok">no changes</Pill>}
        </div>
      </div>
      <p className="muted small">
        {s.latest.objects} objects, {s.fields} fields · snapshot #{s.latest.id} ({s.snapshots} kept), unchanged since{" "}
        {ago(s.latest.discoveredAt)}, last checked {ago(s.latest.checkedAt)}
      </p>
    </article>
  );
}

export function MetadataDetail({ endpoint }: { endpoint: string }) {
  const from = useSearch("from");
  const to = useSearch("to");
  // A new endpoint or pair of snapshots loads at once.
  return <DetailView key={`${endpoint}|${from ?? ""}|${to ?? ""}`} endpoint={endpoint} from={from} to={to} />;
}

function DetailView({ endpoint, from, to }: { endpoint: string; from: string | null; to: string | null }) {
  const { data, error } = usePoll(() => api.metaDetail(endpoint, from, to), 30000);
  const [filter, setFilter] = useState("");
  const [usedOnly, setUsedOnly] = useState(false);

  const select = (param: "from" | "to", value: string) => {
    const q = new URLSearchParams(window.location.search);
    if (value) q.set(param, value);
    else q.delete(param);
    const qs = q.toString();
    navigate(`/metadata/${encodeURIComponent(endpoint)}${qs ? `?${qs}` : ""}`);
  };

  return (
    <section>
      <header className="page-head">
        <p className="small">
          <Link to="/metadata">← Metadata</Link>
        </p>
        <h1 className="mono">{endpoint}</h1>
        {data && (
          <p className="muted">
            {data.connector}
            {data.version ? ` ${data.version}` : ""} · {data.objects.length} objects
          </p>
        )}
      </header>
      <ErrorBanner error={error ?? data?.error ?? null} />
      {data && <Compare data={data} onSelect={select} />}
      {data && <Changes data={data} />}
      {data && data.missing.length > 0 && (
        <>
          <h2>Used but missing</h2>
          <ul className="findings">
            {data.missing.map((m) => (
              <li key={where(m)} className="sev-error">
                <span className="mono">{where(m)}</span>
                <div className="small">used by {m.usedBy.join(", ")}</div>
              </li>
            ))}
          </ul>
        </>
      )}
      {data && (
        <>
          <h2>Objects</h2>
          <div className="review">
            <input aria-label="Filter objects and fields" placeholder="Filter objects and fields" value={filter}
              onChange={(e) => setFilter(e.target.value)} />
            <label className="small">
              <input type="checkbox" checked={usedOnly} onChange={(e) => setUsedOnly(e.target.checked)} /> only fields in use
            </label>
          </div>
          {data.objects.map((o) => (
            <ObjectCard key={o.name} object={o} filter={filter.trim().toLowerCase()} usedOnly={usedOnly} />
          ))}
        </>
      )}
    </section>
  );
}

function Compare({ data, onSelect }: { data: Detail; onSelect: (param: "from" | "to", value: string) => void }) {
  if (data.snapshots.length < 2) {
    return <p className="muted small">One snapshot, discovered {ago(data.to.discoveredAt)}: nothing to compare yet.</p>;
  }
  const label = (id: number) => {
    const s = data.snapshots.find((x) => x.id === id);
    return s ? `#${s.id} · ${new Date(s.discoveredAt).toLocaleString()}` : `#${id}`;
  };
  return (
    <div className="review">
      <label className="small">
        Changes from{" "}
        <select aria-label="Compare from snapshot" value={data.from?.id ?? ""} onChange={(e) => onSelect("from", e.target.value)}>
          {!data.from && <option value="">—</option>}
          {data.snapshots.map((s) => (
            <option key={s.id} value={s.id}>
              {label(s.id)}
            </option>
          ))}
        </select>
      </label>{" "}
      <label className="small">
        to{" "}
        <select aria-label="Compare to snapshot" value={data.to.id} onChange={(e) => onSelect("to", e.target.value)}>
          {data.snapshots.map((s) => (
            <option key={s.id} value={s.id}>
              {label(s.id)}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}

function Changes({ data }: { data: Detail }) {
  if (!data.from) {
    return data.snapshots.length > 1 ? (
      <p className="muted small">Snapshot #{data.to.id} is the oldest kept: there is nothing before it to compare.</p>
    ) : null;
  }
  return (
    <>
      <h2>Changes</h2>
      {data.changes.length === 0 ? (
        <Empty>No changes between these snapshots.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="list wide">
            <thead>
              <tr>
                <th></th>
                <th>Change</th>
                <th>Field</th>
                <th>Values</th>
                <th>Used by</th>
              </tr>
            </thead>
            <tbody>
              {data.changes.map((c) => (
                <tr key={c.kind + where(c)}>
                  <td>{breaks(c) ? <Pill tone="bad">breaking</Pill> : c.breaking ? <Pill tone="warn">not in use</Pill> : <Pill tone="neutral">safe</Pill>}</td>
                  <td>{c.kind.replaceAll("-", " ")}</td>
                  <td className="mono">{where(c)}</td>
                  <td className="mono small">{values(c)}</td>
                  <td className="small">{c.usedBy?.join(", ") ?? <span className="muted">nothing here</span>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

function ObjectCard({ object: o, filter, usedOnly }: { object: MetaObject; filter: string; usedOnly: boolean }) {
  const nameMatches = filter === "" || o.name.toLowerCase().includes(filter) || (o.label ?? "").toLowerCase().includes(filter);
  const fields = o.fields.filter(
    (f) =>
      (!usedOnly || (f.usedBy?.length ?? 0) > 0) &&
      (nameMatches || f.name.toLowerCase().includes(filter) || (f.label ?? "").toLowerCase().includes(filter)),
  );
  if (fields.length === 0 && !(nameMatches && !usedOnly)) return null;
  const used = o.fields.filter((f) => (f.usedBy?.length ?? 0) > 0).length;
  return (
    <article className="card">
      <div className="card-head">
        <div>
          <span className="title mono">{o.name}</span> <span className="muted small">{o.kind}{o.label ? ` · ${o.label}` : ""}</span>
        </div>
        <div className="muted small">
          {o.fields.length} fields, {used} in use
        </div>
      </div>
      {o.usedBy && o.usedBy.length > 0 && <p className="small">Used by {o.usedBy.join(", ")}</p>}
      {o.links && o.links.length > 0 && (
        <p className="muted small">
          Links: {o.links.map((l) => `${l.name} → ${l.to}${l.toField ? `.${l.toField}` : ""}`).join(", ")}
        </p>
      )}
      {fields.length > 0 && (
        <div className="table-wrap">
          <table className="list wide">
            <thead>
              <tr>
                <th>Field</th>
                <th>Type</th>
                <th>Length</th>
                <th>Constraints</th>
                <th>Used by</th>
              </tr>
            </thead>
            <tbody>
              {fields.map((f) => (
                <tr key={f.name}>
                  <td className="mono" title={f.label}>
                    {f.name}
                  </td>
                  <td className="mono small">{f.type}</td>
                  <td className="small">{f.length || ""}</td>
                  <td className="small">{flags(f).join(", ")}</td>
                  <td className="small">{f.usedBy?.join(", ") ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </article>
  );
}
