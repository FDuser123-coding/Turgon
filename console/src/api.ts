import { createDemoApi } from "./demo";
import type { AuditLog, CatalogReport, LinkResult, RunDetail, RunSummary, StewardItem, User } from "./types";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { credentials: "same-origin", ...init });
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  if (!res.ok) {
    const msg = (body as { error?: string } | null)?.error ?? `${res.status} ${res.statusText}`;
    // The session ended: sign in again and come back to this page.
    const login = (body as { login?: string } | null)?.login;
    if (res.status === 401 && login?.startsWith("/auth/login")) {
      window.location.assign(`/auth/login?next=${encodeURIComponent(window.location.pathname + window.location.search)}`);
    }
    throw new ApiError(res.status, msg);
  }
  return body as T;
}

const liveApi = {
  me: () => request<User>("/api/me"),
  runs: () => request<RunSummary[]>("/api/runs"),
  run: (id: string) => request<RunDetail>(`/api/runs/${id.split("/").map(encodeURIComponent).join("/")}`),
  audit: () => request<AuditLog[]>("/api/audit"),
  catalog: () => request<CatalogReport>("/api/catalog"),
  decide: (d: { runId: string; step: string; digest: string; decision: "approve" | "reject"; note?: string }) =>
    request<{ status: string }>("/api/decisions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(d),
    }),
  retry: (r: { id: string; note: string }) =>
    request<{ retried: string }>("/api/runs/retry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(r),
    }),
  steward: () => request<StewardItem[]>("/api/steward"),
  link: (l: { entity: string; system: string; ref: string; master: string; note?: string }) =>
    request<LinkResult>("/api/steward/links", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(l),
    }),
};

// The demo build (Vercel) uses sample data: the real API runs inside the
// customer's environment and is never exposed publicly.
export const demo = __TURGON_DEMO__;
export const api: typeof liveApi = demo ? createDemoApi() : liveApi;
