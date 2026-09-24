import { api } from "../api";
import { Empty, ErrorBanner, Pill } from "../components";
import { display } from "../format";
import { usePoll } from "../hooks";

export function Audit() {
  const { data, error } = usePoll(api.audit, 10000);
  return (
    <section>
      <header className="page-head">
        <h1>Audit</h1>
        <p className="muted">
          Every write, approval and policy decision, in hash-chained logs. The chain is verified each time this page
          loads; any edited or deleted entry breaks it.
        </p>
      </header>
      <ErrorBanner error={error} />
      {data && data.length === 0 && <Empty>No audit logs configured. Start the console with --audit-log.</Empty>}
      {data?.map((log) => (
        <article key={log.file} className="card">
          <div className="card-head">
            <div className="title mono">{log.file}</div>
            {log.ok ? (
              <Pill tone="ok">chain verified · {log.count} entries</Pill>
            ) : (
              <Pill tone="bad">chain broken</Pill>
            )}
          </div>
          {log.error && <div className="banner banner-bad">{log.error}</div>}
          <div className="table-wrap">
          <table className="list">
            <thead>
              <tr>
                <th>#</th>
                <th className="hide-sm">Time</th>
                <th>Actor</th>
                <th>Action</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {log.entries.map((e) => (
                <tr key={e.seq}>
                  <td className="mono">{e.seq}</td>
                  <td className="hide-sm" title={e.time}>
                    {new Date(e.time).toLocaleString()}
                  </td>
                  <td className="mono">{e.actor}</td>
                  <td className="mono">{e.action}</td>
                  <td className="mono small truncate" title={display(e.data)}>
                    {summary(e.data)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        </article>
      ))}
    </section>
  );
}

function summary(d: Record<string, unknown> | undefined): string {
  if (!d) return "";
  const parts = [d.target && d.operation ? `${d.operation} on ${d.target}` : "", d.status ? `${d.status}` : "", d.key ? `key ${d.key}` : ""];
  const decision = d.decision as { allow?: boolean; requireApproval?: boolean } | undefined;
  if (decision) parts.push(`allow=${decision.allow} approval=${decision.requireApproval}`);
  if (d.note) parts.push(`“${String(d.note)}”`);
  if (d.error) parts.push(String(d.error));
  const s = parts.filter(Boolean).join(" · ");
  return s || display(d);
}
