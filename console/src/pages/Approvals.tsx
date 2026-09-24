import { useState } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Json, Link, PreviewTable, Risk } from "../components";
import { ago } from "../format";
import { refreshAll, usePoll } from "../hooks";
import type { RunSummary, User } from "../types";

export function Approvals({ user }: { user: User }) {
  const { data, error, refresh } = usePoll(api.runs);
  const waiting = (data ?? []).filter((r): r is RunSummary & { pending: NonNullable<RunSummary["pending"]> } => !!r.pending);
  return (
    <section>
      <header className="page-head">
        <h1>Approvals</h1>
        <p className="muted">
          Writes into systems of record that policy wants a person to approve. Each shows the dry-run the
          target produced; your decision applies to exactly that request.
        </p>
      </header>
      <ErrorBanner error={error} />
      {data && waiting.length === 0 && <Empty>Nothing is waiting for approval.</Empty>}
      {waiting.map((r) => (
        <ApprovalCard key={r.id + r.pending.digest} run={r} user={user} onDone={refresh} />
      ))}
    </section>
  );
}

function ApprovalCard({
  run,
  user,
  onDone,
}: {
  run: RunSummary & { pending: NonNullable<RunSummary["pending"]> };
  user: User;
  onDone: () => void;
}) {
  const p = run.pending;
  const req = p.request;
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const canApprove = user.roles.includes("approver");
  const own = req.subject.id === user.id || req.subject.onBehalfOf === user.id;

  async function decide(decision: "approve" | "reject") {
    setBusy(true);
    setError(null);
    try {
      await api.decide({ runId: run.id, step: p.step, digest: p.digest, decision, note: note || undefined });
      onDone();
      refreshAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setBusy(false);
    }
  }

  return (
    <article className="card">
      <div className="card-head">
        <div>
          <div className="title">
            <span className="mono">{req.operation}</span> on <span className="mono">{req.target}</span>
          </div>
          <div className="muted small">
            <Link to={`/runs/${run.id}`}>{run.id}</Link> · step {p.step} · waiting {ago(p.since)}
          </div>
        </div>
        <Risk risk={req.risk} />
      </div>

      <dl className="facts">
        <dt>Why approval</dt>
        <dd>{(p.reasons ?? []).join("; ") || "required by the recipe"}</dd>
        <dt>Requested by</dt>
        <dd className="mono">
          {req.subject.id}
          {req.subject.onBehalfOf ? ` on behalf of ${req.subject.onBehalfOf}` : ""}
        </dd>
        <dt>Idempotency key</dt>
        <dd className="mono">{req.idempotencyKey}</dd>
        {req.amount ? (
          <>
            <dt>Amount</dt>
            <dd>{req.amount.toLocaleString()}</dd>
          </>
        ) : null}
        <dt>If undone</dt>
        <dd className="mono">{req.compensation || "—"}</dd>
      </dl>

      <PreviewTable preview={p.preview} />

      <details>
        <summary>Full request payload</summary>
        <Json value={req.payload} />
      </details>

      <ErrorBanner error={error} />
      {canApprove && !own ? (
        <div className="decide">
          <input
            aria-label="Note"
            placeholder="Note for the audit log (optional)"
            value={note}
            onChange={(e) => setNote(e.target.value)}
            maxLength={500}
          />
          <button className="btn btn-bad" disabled={busy} onClick={() => void decide("reject")}>
            Reject
          </button>
          <button className="btn btn-ok" disabled={busy} onClick={() => void decide("approve")}>
            Approve
          </button>
        </div>
      ) : (
        <p className="muted small">
          {own ? "You cannot approve a write made on your behalf." : "You can view this request but not decide on it."}
        </p>
      )}
      <p className="muted small mono">request {p.digest.slice(0, 16)}</p>
    </article>
  );
}
