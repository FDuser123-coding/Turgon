// Pure helpers, unit-tested in format.test.ts.

export function ago(iso: string | undefined, now: Date = new Date()): string {
  if (!iso) return "";
  const s = Math.round((now.getTime() - new Date(iso).getTime()) / 1000);
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min ago`;
  const h = Math.round(m / 60);
  if (h < 48) return `${h} h ago`;
  return `${Math.round(h / 24)} days ago`;
}

export function duration(start: string, end?: string, now: Date = new Date()): string {
  const ms = (end ? new Date(end) : now).getTime() - new Date(start).getTime();
  if (ms < 1000) return `${Math.max(ms, 0)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} min ${Math.round(s % 60)} s`;
  return `${Math.floor(m / 60)} h ${m % 60} min`;
}

export function display(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

export interface PreviewRow {
  field: string;
  current?: unknown;
  proposed: unknown;
  changed: boolean;
}

export interface Preview {
  kind: "rollback" | "diff" | "raw";
  title: string;
  rows: PreviewRow[];
}

// toPreview turns a connector's dry-run preview into rows an approver can
// scan: the row a rolled-back insert produced, or current vs proposed values.
export function toPreview(p: unknown): Preview | null {
  if (!p || typeof p !== "object") return null;
  const o = p as Record<string, unknown>;
  if (o.mode === "rollback" && o.row && typeof o.row === "object") {
    const row = o.row as Record<string, unknown>;
    return {
      kind: "rollback",
      title: "Row the write produced in a rolled-back transaction",
      rows: Object.keys(row)
        .sort()
        .map((field) => ({ field, proposed: row[field], changed: true })),
    };
  }
  if (o.mode === "preview" && o.proposed && typeof o.proposed === "object") {
    const proposed = o.proposed as Record<string, unknown>;
    const current = (o.current ?? {}) as Record<string, unknown>;
    return {
      kind: "diff",
      title: `${display(o.sobject)} ${display(o.id)}: current and proposed values`,
      rows: Object.keys(proposed)
        .sort()
        .map((field) => ({
          field,
          current: current[field],
          proposed: proposed[field],
          changed: JSON.stringify(current[field] ?? null) !== JSON.stringify(proposed[field] ?? null),
        })),
    };
  }
  return { kind: "raw", title: "Preview", rows: [{ field: "preview", proposed: p, changed: true }] };
}

export function statusTone(status: string): "ok" | "bad" | "warn" | "neutral" {
  switch (status) {
    case "completed":
    case "committed":
      return "ok";
    case "failed":
    case "terminated":
    case "timed_out":
    case "rejected":
    case "denied":
      return "bad";
    case "running":
    case "duplicate":
      return "warn";
    default:
      return "neutral";
  }
}
