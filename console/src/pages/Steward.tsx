import { useState } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Json, Link } from "../components";
import { ago } from "../format";
import { refreshAll, usePoll } from "../hooks";
import type { StewardItem, User } from "../types";

export function Steward({ user }: { user: User }) {
  const { data, error, refresh } = usePoll(api.steward);
  return (
    <section>
      <header className="page-head">
        <h1>Data steward</h1>
        <p className="muted">
          Records that arrived without a master record. Link each to the master record it is, and every run
          waiting on it starts again. Each link is recorded in the audit log under your name.
        </p>
      </header>
      <ErrorBanner error={error} />
      {data && data.length === 0 && <Empty>Every record has its master record.</Empty>}
      {(data ?? []).map((it) => (
        <StewardCard key={it.entity + it.system + it.ref} item={it} user={user} onDone={refresh} />
      ))}
    </section>
  );
}

function StewardCard({ item, user, onDone }: { item: StewardItem; user: User; onDone: () => void }) {
  const [master, setMaster] = useState("");
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState<string | null>(null);
  const canLink = user.roles.includes("steward");
  const sample = item.runs.find((r) => r.event)?.event;

  async function link() {
    setBusy(true);
    setError(null);
    try {
      const res = await api.link({ entity: item.entity, system: item.system, ref: item.ref, master: master.trim(), note: note || undefined });
      const failed = Object.entries(res.failed ?? {});
      setDone(
        `Linked to ${master.trim()}; ${res.retried.length} run(s) started again` +
          (failed.length ? `; ${failed.length} could not be: ${failed.map(([id, e]) => `${id} (${e})`).join(", ")}` : "."),
      );
      onDone();
      refreshAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setBusy(false);
  }

  return (
    <article className="card">
      <div className="card-head">
        <div>
          <div className="title">
            <span className="mono">{item.ref}</span>
          </div>
          <div className="muted small">
            {item.entity} from <span className="mono">{item.system}</span> · waiting {ago(item.since)}
          </div>
        </div>
      </div>

      <dl className="facts">
        <dt>Waiting runs</dt>
        <dd>
          {item.runs.map((r, i) => (
            <span key={r.id}>
              {i > 0 && ", "}
              <Link to={`/runs/${r.id}`}>{r.id}</Link>
            </span>
          ))}
        </dd>
      </dl>
      {item.suggestions && item.suggestions.length > 0 && (
        <div className="suggestions">
          <div className="muted small">Likely the same {item.entity.toLowerCase()} as:</div>
          {item.suggestions.map((sg) => (
            <button
              key={sg.master}
              type="button"
              className={master === sg.master ? "suggestion chosen" : "suggestion"}
              disabled={!canLink || !!done}
              onClick={() => setMaster(sg.master)}
              title="Use this master record"
            >
              <span className="mono">{sg.master}</span>
              <span className="score">{Math.round(sg.score * 100)}%</span>
              <span className="muted small">{sg.reasons.join(", ")}</span>
            </button>
          ))}
        </div>
      )}
      {sample && (
        <details>
          <summary>Source record ({sample.name})</summary>
          <Json value={sample.payload} />
        </details>
      )}

      <ErrorBanner error={error} />
      {done ? (
        <p className="small">{done}</p>
      ) : canLink ? (
        <form
          className="decide"
          onSubmit={(e) => {
            e.preventDefault();
            void link();
          }}
        >
          <input
            aria-label={`Master ${item.entity} ID`}
            placeholder={`Master ${item.entity.toLowerCase()} ID, e.g. C-100`}
            value={master}
            onChange={(e) => setMaster(e.target.value)}
            maxLength={200}
            required
          />
          <input aria-label="Note" placeholder="Why (optional)" value={note} onChange={(e) => setNote(e.target.value)} maxLength={500} />
          <button className="btn btn-ok" type="submit" disabled={busy || master.trim() === ""}>
            Link and retry
          </button>
        </form>
      ) : (
        <p className="muted small">Only data stewards can link records.</p>
      )}
    </article>
  );
}
