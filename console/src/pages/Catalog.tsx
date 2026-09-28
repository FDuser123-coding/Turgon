import { useState } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Pill } from "../components";
import { usePoll } from "../hooks";
import type { Report, ReviewItem, User } from "../types";

export function Catalog({ user }: { user: User }) {
  const { data, error, refresh } = usePoll(api.catalog, 30000);
  const canReview = user.roles.includes("steward");
  const reviews = (data?.reports ?? []).flatMap(collectReviews);
  return (
    <section>
      <header className="page-head">
        <h1>Catalog</h1>
        <p className="muted">
          What the verifier says about each recipe and stack: its one-click level (L0 certified to L3 engineered),
          whether it can deploy, and mapped fields waiting for a person. A data steward approves or rejects each
          field; the decision holds for that expression only and is recorded in the audit log under their name.
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
              <th>Review</th>
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
                <td>{canReview ? <ReviewForm item={r} onDone={refresh} /> : <span className="muted small">Data stewards review</span>}</td>
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

function ReviewForm({ item, onDone }: { item: ReviewItem; onDone: () => void }) {
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function decide(decision: "approved" | "rejected") {
    setBusy(true);
    setError(null);
    try {
      await api.review({ mapping: item.mapping, target: item.target, expression: item.expression, decision, note: note.trim() || undefined });
      onDone();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setBusy(false);
  }

  return (
    <div className="review">
      <input aria-label={`Note on ${item.target}`} placeholder="Note (needed to reject)" value={note}
        onChange={(e) => setNote(e.target.value)} maxLength={1000} />
      <button className="btn btn-ok" type="button" disabled={busy} onClick={() => void decide("approved")}>
        Approve
      </button>
      <button className="btn btn-bad" type="button" disabled={busy || note.trim() === ""} onClick={() => void decide("rejected")}
        title={note.trim() === "" ? "Say why, so the mapping's author can fix it" : undefined}>
        Reject
      </button>
      <ErrorBanner error={error} />
    </div>
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
