// Mirrors the JSON of pkg/console, pkg/engine and pkg/verifier.

export interface User {
  id: string;
  roles: string[];
  // Where to sign out, when the console signs people in itself (OIDC).
  logout?: string;
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

export interface Link {
  entity: string;
  system: string;
  ref: string;
}

export interface Suggestion {
  master: string;
  score: number;
  reasons: string[];
}

export interface UnresolvedRun {
  id: string;
  workflow: string;
  started: string;
  failed: string;
  link: Link & { attributes?: Record<string, string>; suggestions?: Suggestion[] };
  event?: { id: string; position: number; name: string; payload: unknown };
}

export interface StewardItem extends Link {
  attributes?: Record<string, string>;
  suggestions?: Suggestion[];
  since: string;
  runs: UnresolvedRun[];
}

export interface LinkResult {
  retried: string[];
  failed?: Record<string, string>;
}

// The map of connected systems and the flows between them (/api/integrations).
export interface Integrations {
  error?: string;
  systems: IntegrationSystem[];
  flows: Flow[];
}

export interface IntegrationSystem {
  name: string;
  description?: string;
  connector: string;
  product?: string;
  role: "source" | "target" | "both" | "unused";
  events: { name: string; entity?: string; delivery: string }[];
  operations: string[];
  flows: string[];
}

export interface Flow {
  name: string;
  version: string;
  description?: string;
  level?: string;
  deployable: boolean;
  trigger: { system: string; event: string; delivery: string };
  steps: FlowStep[];
  targets: string[];
  slo?: string;
  problem?: string;
}

export interface FlowStep {
  kind: "map" | "resolve" | "write";
  mapping?: string;
  from?: string;
  to?: string;
  fields?: number;
  entity?: string;
  strategy?: string;
  system?: string;
  operation?: string;
  risk?: string;
  approval?: string;
  simulation?: string;
  compensation?: string;
  plugins?: string[];
}

// The metadata graph: what each system holds, as `turgon discover` found it.
export interface MetaSnapshot {
  id: number;
  endpoint: string;
  connector: string;
  digest: string;
  discoveredAt: string;
  checkedAt: string;
  objects: number;
}

export interface MetaSystem {
  endpoint: string;
  connector: string;
  version?: string;
  latest: MetaSnapshot;
  snapshots: number;
  fields: number;
  changes: number;
  breaking: number;
  missing: number;
}

export interface MetaOverview {
  error?: string;
  systems: MetaSystem[];
}

export interface MetaChange {
  kind: string;
  object: string;
  field?: string;
  old?: string;
  new?: string;
  breaking: boolean;
  usedBy?: string[];
}

export interface MetaMissing {
  object: string;
  field?: string;
  usedBy: string[];
}

export interface MetaField {
  name: string;
  type: string;
  label?: string;
  length?: number;
  required?: boolean;
  key?: boolean;
  readOnly?: boolean;
  usedBy?: string[];
}

export interface MetaObject {
  name: string;
  kind: string;
  label?: string;
  fields: MetaField[];
  links?: { name: string; to: string; toField?: string }[];
  usedBy?: string[];
}

export interface MetaDetail {
  error?: string;
  endpoint: string;
  connector: string;
  version?: string;
  snapshots: MetaSnapshot[];
  to: MetaSnapshot;
  from?: MetaSnapshot;
  changes: MetaChange[];
  missing: MetaMissing[];
  objects: MetaObject[];
  events?: Record<string, string>;
}
