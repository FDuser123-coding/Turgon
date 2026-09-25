import { useCallback, useState } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Json, Link, Pill, Status } from "../components";
import { ago, display, duration } from "../format";
import { refreshAll, usePoll } from "../hooks";
import type { User } from "../types";

// Runs in these states can be started again (console.Retryable).
export const retryable = (status: string) => ["failed", "timed_out", "terminated", "canceled"].includes(status);

export function Runs() {
  const { data, error } = usePoll(api.runs);
  return (
    <section>
      <header className="page-head">
        <h1>Runs</h1>
        <p className="muted">One run per source event or agent write. A failed recipe run can be retried.</p>
      </header>
      <ErrorBanner error={error} />
      {data && data.length === 0 && <Empty>No runs yet. Runs start when a source emits an event.</Empty>}
      {data && data.length > 0 && (
        <div className="table-wrap">
          <table className="list">
          <thead>
            <tr>
              <th>Status</th>
              <th>Run</th>
              <th>Started</th>
              <th className="hide-sm">Duration</th>
            </tr>
          </thead>
          <tbody>
            {data.map((r) => (
              <tr key={r.id + r.runId}>
                <td>
                  <Status status={r.status} /> {r.pending && <Pill tone="info">awaiting approval</Pill>}
                </td>
                <td>
                  <Link to={`/runs/${r.id}`} className="mono">
                    {r.id}
                  </Link>
                </td>
                <td title={r.started}>{ago(r.started)}</td>
                <td className="hide-sm">{duration(r.started, r.closed)}</td>
              </tr>
            ))}
          </tbody>
        </table>
          </div>
      )}
    </section>
  );
}

export function RunDetail({ id, user }: { id: string; user: User }) {
  const load = useCallback(() => api.run(id), [id]);
  const { data: run, error, refresh } = usePoll(load);
  return (
    <section>
      <header className="page-head">
        <p className="small">
          <Link to="/runs">← Runs</Link>
        </p>
        <h1 className="mono">{id}</h1>
      </header>
      <ErrorBanner error={error} />
      {run && (
        <>
          <dl className="facts">
            <dt>Status</dt>
            <dd>
              <Status status={run.status} /> {run.pending && <Link to="/">awaiting approval of {run.pending.step}</Link>}
            </dd>
            <dt>Started</dt>
            <dd>
              {new Date(run.started).toLocaleString()} ({duration(run.started, run.closed)})
            </dd>
            <dt>Spec</dt>
            <dd className="mono small">{run.specDigest ?? "—"}</dd>
          </dl>
          {run.failure && (
            <div className="banner banner-bad">
              <strong>{run.failureType ?? "Failed"}</strong>: {run.failure}
            </div>
          )}
          {retryable(run.status) && <Retry id={id} user={user} onDone={refresh} />}
          <h2>Writes</h2>
          {run.result?.writes?.length ? (
            <div className="table-wrap">
          <table className="list">
              <thead>
                <tr>
                  <th>Step</th>
                  <th>Write</th>
                  <th>Status</th>
                  <th>Result</th>
                </tr>
              </thead>
              <tbody>
                {run.result.writes.map((w) => (
                  <tr key={w.step}>
                    <td className="mono">{w.step}</td>
                    <td className="mono">
                      {w.operation} on {w.endpoint}
                    </td>
                    <td>
                      <Status status={w.status} />
                    </td>
                    <td className="mono small truncate" title={display(w.result)}>
                      {display(w.result)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          ) : (
            <p className="muted">{run.status === "running" ? "Writes appear when the run completes." : "No writes were committed."}</p>
          )}
          {run.request ? (
            <>
              <h2>Agent request</h2>
              <p className="muted small">
                {run.request.subject.id}
                {run.request.subject.onBehalfOf ? ` on behalf of ${run.request.subject.onBehalfOf}` : ""} ·{" "}
                {run.request.tool} · {run.request.risk} risk
                {run.request.reason ? ` · ${run.request.reason}` : ""}
              </p>
              <Json value={run.request.payload} />
            </>
          ) : (
            <>
              <h2>Source event</h2>
              {run.event ? (
                <>
                  <p className="muted small">
                    {run.event.name} · id {run.event.id} · position {run.event.position}
                  </p>
                  <Json value={run.event.payload} />
                </>
              ) : (
                <p className="muted">Unavailable.</p>
              )}
            </>
          )}
        </>
      )}
    </section>
  );
}

// Retry starts a failed run again from its event. Writes that already
// happened are not repeated: the run reuses their idempotency keys.
function Retry({ id, user, onDone }: { id: string; user: User; onDone: () => void }) {
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  if (!user.roles.includes("operator")) {
    return <p className="muted small">Operators can retry this run.</p>;
  }
  async function retry() {
    setBusy(true);
    setError(null);
    try {
      await api.retry({ id, note: note.trim() });
      setNote("");
      onDone();
      refreshAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setBusy(false);
  }
  return (
    <>
      <ErrorBanner error={error} />
      <form
        className="decide"
        onSubmit={(e) => {
          e.preventDefault();
          void retry();
        }}
      >
        <input
          aria-label="Why retry"
          placeholder="Why retry, e.g. the approver is back (recorded in the audit log)"
          value={note}
          onChange={(e) => setNote(e.target.value)}
          maxLength={500}
          required
        />
        <button className="btn btn-ok" type="submit" disabled={busy || note.trim() === ""}>
          Retry run
        </button>
      </form>
      <p className="muted small">
        The run starts again from its event. Writes it already made are not repeated.
      </p>
    </>
  );
}
