import { useState } from "react";
import { api } from "../api";
import { Empty, ErrorBanner, Json, Link, PreviewTable, Risk } from "../components";
import { ago } from "../format";
import { refreshAll, usePoll } from "../hooks";
import type { HeldRequest, RunSummary, User } from "../types";

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
      <Orphaned user={user} />
    </section>
  );
}

// Requests whose run ended without closing them: the runs waiting on them
// sleep until the request expires, unless an operator releases it.
function Orphaned({ user }: { user: User }) {
  const { data, error, refresh } = usePoll(api.ledger);
  const orphaned = (data ?? []).filter((h) => h.orphaned);
  if (!error && orphaned.length === 0) return null;
  return (
    <>
      <h2>Orphaned requests</h2>
      <p className="muted">
        The run that asked for these approvals ended without closing its request (it was terminated, say). Runs for
        later changes of the same record wait on it until it expires. Releasing it wakes them, and the newest asks.
      </p>
      <ErrorBanner error={error} />
      {orphaned.map((h) => (
        <OrphanCard key={h.key} held={h} user={user} onDone={refresh} />
      ))}
    </>
  );
}

function OrphanCard({ held, user, onDone }: { held: HeldRequest; user: User; onDone: () => void }) {
  const [note, setNote] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const canRelease = user.roles.includes("operator");
  async function release() {
    setBusy(true);
    setError(null);
    try {
      await api.release({ key: held.key, holder: held.holder, note });
      setNote("");
      onDone();
      refreshAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <article className="card">
      <div className="card-head">
        <div>
          <div className="title mono">{held.key}</div>
          <div className="muted small">
            held by <Link to={`/runs/${held.holder}`}>{held.holder}</Link> ({held.holderStatus}) · expires{" "}
            {new Date(held.until).toLocaleString()}
          </div>
        </div>
      </div>
      <dl className="facts">
        <dt>Waiting</dt>
        <dd>
          {(held.waiters ?? []).length === 0
            ? "no run waits on it"
            : (held.waiters ?? []).map((w, i) => (
                <span key={w}>
                  {i > 0 ? ", " : ""}
                  <Link to={`/runs/${w}`} className="mono">
                    {w}
                  </Link>
                </span>
              ))}
        </dd>
      </dl>
      <ErrorBanner error={error} />
      {canRelease ? (
        <div className="decide">
          <input
            aria-label="Why release it"
            placeholder="Why release it (recorded in the audit log)"
            value={note}
            onChange={(e) => setNote(e.target.value)}
            maxLength={500}
          />
          <button className="btn btn-ok" disabled={busy || !note.trim()} onClick={() => void release()}>
            Release
          </button>
        </div>
      ) : (
        <p className="muted small">An operator can release it.</p>
      )}
    </article>
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
        {req.subject.agent && req.reason ? (
          <>
            <dt>Agent's reason</dt>
            <dd>{req.reason}</dd>
          </>
        ) : null}
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
        {p.waiting?.length ? (
          <>
            <dt>Newer changes</dt>
            <dd>
              {p.waiting.map((w, i) => (
                <span key={w}>
                  {i > 0 ? ", " : ""}
                  <Link to={`/runs/${w}`} className="mono">
                    {w}
                  </Link>
                </span>
              ))}{" "}
              <span className="muted small">
                {p.waiting.length === 1 ? "waits" : "wait"} on this decision: approving writes this version; rejecting
                lets the newest change ask with its own data.
              </span>
            </dd>
          </>
        ) : null}
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
