import { api } from "../api";
import { Empty, ErrorBanner, Pill } from "../components";
import { usePoll } from "../hooks";
import type { Report } from "../types";

export function Catalog() {
  const { data, error } = usePoll(api.catalog, 30000);
  const reviews = (data?.reports ?? []).flatMap(collectReviews);
  return (
    <section>
      <header className="page-head">
        <h1>Catalog</h1>
        <p className="muted">
          What the verifier says about each recipe and stack: its one-click level (L0 certified to L3 engineered),
          whether it can deploy, and mapped fields waiting for a person.
        </p>
      </header>
      <ErrorBanner error={error ?? data?.error ?? null} />
      <h2>Mapping review queue</h2>
      {reviews.length === 0 ? (
        <Empty>No mapped fields are waiting for review.</Empty>
      ) : (
        <div className="table-wrap">
          <table className="list">
          <thead>
            <tr>
              <th>Mapping</th>
              <th>Field</th>
              <th>Expression</th>
              <th>Origin</th>
              <th>Confidence</th>
            </tr>
          </thead>
          <tbody>
            {reviews.map((r) => (
              <tr key={r.mapping + r.target}>
                <td className="mono">{r.mapping}</td>
                <td className="mono">{r.target}</td>
                <td className="mono small" title={r.rationale}>
                  {r.expression}
                </td>
                <td>{r.origin}</td>
                <td>{(r.confidence * 100).toFixed(0)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
          </div>
      )}
      <h2>Recipes and stacks</h2>
      {data?.reports.length === 0 && <Empty>No catalog configured. Start the console with --catalog.</Empty>}
      {data?.reports.map((r) => <ReportCard key={r.subject} report={r} />)}
    </section>
  );
}

function collectReviews(r: Report): NonNullable<Report["reviewQueue"]> {
  return [...(r.reviewQueue ?? []), ...(r.children ?? []).flatMap(collectReviews)];
}

function ReportCard({ report, nested }: { report: Report; nested?: boolean }) {
  const findings = (report.findings ?? []).filter((f) => f.severity !== "info");
  return (
    <article className={nested ? "nested" : "card"}>
      <div className="card-head">
        <div className="title mono">{report.subject}</div>
        <div>
          <Pill tone={report.level === "L0" ? "ok" : report.level === "L3" ? "bad" : "info"}>{report.level}</Pill>{" "}
          <Pill tone={report.deployable ? "ok" : "bad"}>{report.deployable ? "deployable" : "blocked"}</Pill>
        </div>
      </div>
      {findings.length > 0 && (
        <ul className="findings">
          {findings.map((f, i) => (
            <li key={i} className={`sev-${f.severity}`}>
              <span className="mono small">
                {f.severity} · {f.stage}
                {f.path ? ` · ${f.path}` : ""}
              </span>
              <div>{f.message}</div>
            </li>
          ))}
        </ul>
      )}
      {report.children?.map((c) => <ReportCard key={c.subject} report={c} nested />)}
    </article>
  );
}
