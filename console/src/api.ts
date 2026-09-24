import type { AuditLog, CatalogReport, RunDetail, RunSummary, User } from "./types";

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
    throw new ApiError(res.status, msg);
  }
  return body as T;
}

export const api = {
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
};
