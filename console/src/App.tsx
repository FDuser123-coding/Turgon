import { useEffect, useState } from "react";
import { api, demo } from "./api";
import { ErrorBanner, Link } from "./components";
import { usePath, usePoll } from "./hooks";
import { Approvals } from "./pages/Approvals";
import { Audit } from "./pages/Audit";
import { Catalog } from "./pages/Catalog";
import { RunDetail, Runs } from "./pages/Runs";
import { Steward } from "./pages/Steward";
import type { User } from "./types";

export function App() {
  const path = usePath();
  const [user, setUser] = useState<User | null>(null);
  const [error, setError] = useState<string | null>(null);
  const runs = usePoll(api.runs);
  const waiting = (runs.data ?? []).filter((r) => r.pending).length;
  const steward = usePoll(api.steward);
  const unlinked = (steward.data ?? []).length;

  useEffect(() => {
    api.me().then(setUser, (e: Error) => setError(e.message));
  }, []);

  useEffect(() => {
    document.title = waiting > 0 ? `(${waiting}) Turgon console` : "Turgon console";
  }, [waiting]);

  let page;
  if (!user) page = <ErrorBanner error={error} />;
  else if (path === "/" || path === "/approvals") page = <Approvals user={user} />;
  else if (path === "/steward") page = <Steward user={user} />;
  else if (path === "/runs") page = <Runs />;
  else if (path.startsWith("/runs/")) page = <RunDetail id={decodeURIComponent(path.slice("/runs/".length))} user={user} />;
  else if (path === "/audit") page = <Audit />;
  else if (path === "/catalog") page = <Catalog />;
  else page = <p>Not found.</p>;

  const tab = (to: string, label: string, badge?: number) => {
    const active = to === "/" ? path === "/" || path === "/approvals" : path.startsWith(to);
    return (
      <Link to={to} className={active ? "tab active" : "tab"}>
        {label}
        {badge ? <span className="badge">{badge}</span> : null}
      </Link>
    );
  };

  return (
    <>
      <nav className="top">
        <span className="brand">Turgon</span>
        {tab("/", "Approvals", waiting)}
        {tab("/steward", "Steward", unlinked)}
        {tab("/runs", "Runs")}
        {tab("/audit", "Audit")}
        {tab("/catalog", "Catalog")}
        <span className="spacer" />
        {user && (
          <span className="muted small" title={user.roles.join(", ")}>
            {user.id}
          </span>
        )}
      </nav>
      {demo && (
        <div className="demo-banner" role="note">
          Demo with sample data: no systems are connected, and decisions stay in this browser tab.
        </div>
      )}
      <main>{page}</main>
    </>
  );
}
