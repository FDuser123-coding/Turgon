import { useCallback } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Json, Link, Pill, Status } from "../components";
import { ago, display, duration } from "../format";
import { usePoll } from "../hooks";

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

export function RunDetail({ id }: { id: string }) {
  const load = useCallback(() => api.run(id), [id]);
  const { data: run, error } = usePoll(load);
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
