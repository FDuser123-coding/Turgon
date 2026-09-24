import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";

const REFRESH = "turgon:refresh";

// refreshAll makes every polling view reload now, e.g. after a decision.
export function refreshAll() {
  window.dispatchEvent(new Event(REFRESH));
}

// usePoll loads data now, every intervalMs while the tab is visible, and
// whenever refreshAll is called.
export function usePoll<T>(load: () => Promise<T>, intervalMs = 5000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const loadRef = useRef(load);
  loadRef.current = load;

  const refresh = useCallback(async () => {
    try {
      setData(await loadRef.current());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    void refresh();
    const t = window.setInterval(() => {
      if (document.visibilityState === "visible") void refresh();
    }, intervalMs);
    const onRefresh = () => void refresh();
    window.addEventListener(REFRESH, onRefresh);
    return () => {
      window.clearInterval(t);
      window.removeEventListener(REFRESH, onRefresh);
    };
  }, [refresh, intervalMs]);

  return { data, error, refresh };
}

// A minimal router on the History API.
const listeners = new Set<() => void>();
window.addEventListener("popstate", () => listeners.forEach((l) => l()));

export function navigate(path: string) {
  if (path !== window.location.pathname) {
    window.history.pushState(null, "", path);
    listeners.forEach((l) => l());
  }
}

export function usePath(): string {
  return useSyncExternalStore(
    (cb) => {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    () => window.location.pathname,
  );
}
