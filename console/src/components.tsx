import type { ReactNode } from "react";
import { display, statusTone, toPreview } from "./format";
import { navigate } from "./hooks";

export function Pill({ tone, children }: { tone: "ok" | "bad" | "warn" | "neutral" | "info"; children: ReactNode }) {
  return <span className={`pill pill-${tone}`}>{children}</span>;
}

export function Status({ status }: { status: string }) {
  return <Pill tone={statusTone(status)}>{status.replace("_", " ")}</Pill>;
}

export function Risk({ risk }: { risk: string }) {
  return <Pill tone={risk === "high" ? "bad" : risk === "low" ? "warn" : "neutral"}>{risk} risk</Pill>;
}

export function Link({ to, children, className }: { to: string; children: ReactNode; className?: string }) {
  return (
    <a
      href={to}
      className={className}
      onClick={(e) => {
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
        e.preventDefault();
        navigate(to);
      }}
    >
      {children}
    </a>
  );
}

export function ErrorBanner({ error }: { error: string | null }) {
  if (!error) return null;
  return (
    <div className="banner banner-bad" role="alert">
      {error}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

export function Json({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>;
}

export function PreviewTable({ preview }: { preview: unknown }) {
  const p = toPreview(preview);
  if (!p) return <p className="muted">The target cannot simulate this write; no preview is available.</p>;
  if (p.kind === "raw") return <Json value={preview} />;
  return (
    <div>
      <p className="muted small">
        {p.title}
        {p.kind === "rollback" && ". Values the database generates, such as ids, will differ at commit."}
      </p>
      <div className="table-wrap">
        <table className="kv">
        <thead>
          <tr>
            <th>Field</th>
            {p.kind === "diff" && <th>Current</th>}
            <th>{p.kind === "diff" ? "Proposed" : "Value"}</th>
          </tr>
        </thead>
        <tbody>
          {p.rows.map((r) => (
            <tr key={r.field} className={p.kind === "diff" && r.changed ? "changed" : undefined}>
              <td className="mono">{r.field}</td>
              {p.kind === "diff" && <td className="mono">{display(r.current)}</td>}
              <td className="mono">{display(r.proposed)}</td>
            </tr>
          ))}
        </tbody>
      </table>
        </div>
    </div>
  );
}
