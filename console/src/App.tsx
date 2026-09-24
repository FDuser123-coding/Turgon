import { useEffect, useState } from "react";
import { api } from "./api";
import { ErrorBanner, Link } from "./components";
import { usePath, usePoll } from "./hooks";
import { Approvals } from "./pages/Approvals";
import { Audit } from "./pages/Audit";
import { Catalog } from "./pages/Catalog";
import { RunDetail, Runs } from "./pages/Runs";
import type { User } from "./types";

export function App() {
  const path = usePath();
  const [user, setUser] = useState<User | null>(null);
  const [error, setError] = useState<string | null>(null);
  const runs = usePoll(api.runs);
  const waiting = (runs.data ?? []).filter((r) => r.pending).length;

  useEffect(() => {
    api.me().then(setUser, (e: Error) => setError(e.message));
  }, []);

  useEffect(() => {
    document.title = waiting > 0 ? `(${waiting}) Porter console` : "Porter console";
  }, [waiting]);

  let page;
  if (!user) page = <ErrorBanner error={error} />;
  else if (path === "/" || path === "/approvals") page = <Approvals user={user} />;
  else if (path === "/runs") page = <Runs />;
  else if (path.startsWith("/runs/")) page = <RunDetail id={decodeURIComponent(path.slice("/runs/".length))} />;
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
        <span className="brand">Porter</span>
        {tab("/", "Approvals", waiting)}
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
      <main>{page}</main>
    </>
  );
}
