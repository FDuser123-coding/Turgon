// Mirrors the JSON of pkg/console, pkg/engine and pkg/verifier.

export interface User {
  id: string;
  roles: string[];
}

export interface Subject {
  id: string;
  roles: string[] | null;
  agent?: boolean;
  onBehalfOf?: string;
}

export interface WriteRequest {
  target: string;
  operation: string;
  tool?: string;
  risk: "read" | "low" | "high";
  subject: Subject;
  idempotencyKey: string;
  payload: unknown;
  entity?: string;
  amount?: number;
  simulate?: boolean;
  compensation?: string;
  reason?: string;
}

export interface PendingApproval {
  step: string;
  digest: string;
  request: WriteRequest;
  preview?: unknown;
  reasons?: string[];
  since: string;
}

export interface RunSummary {
  id: string;
  runId: string;
  workflow: string;
  status: string;
  started: string;
  closed?: string;
  pending?: PendingApproval;
}

export interface WriteRecord {
  step: string;
  endpoint: string;
  operation: string;
  status: string;
  result?: unknown;
}

export interface RunDetail extends RunSummary {
  specDigest?: string;
  event?: { id: string; position: number; name: string; payload: unknown };
  request?: WriteRequest;
  result?: { writes: WriteRecord[] | null };
  failure?: string;
  failureType?: string;
}

export interface AuditEntry {
  seq: number;
  time: string;
  actor: string;
  action: string;
  data?: Record<string, unknown>;
  prev: string;
  hash: string;
}

export interface AuditLog {
  file: string;
  ok: boolean;
  error?: string;
  count: number;
  head?: string;
  entries: AuditEntry[];
}

export interface Finding {
  stage: string;
  severity: "error" | "review" | "warning" | "info";
  path?: string;
  message: string;
}

export interface ReviewItem {
  mapping: string;
  target: string;
  expression: string;
  origin: string;
  confidence: number;
  rationale?: string;
}

export interface Report {
  subject: string;
  level: string;
  deployable: boolean;
  findings?: Finding[];
  reviewQueue?: ReviewItem[];
  children?: Report[];
}

export interface CatalogReport {
  error?: string;
  reports: Report[];
}
