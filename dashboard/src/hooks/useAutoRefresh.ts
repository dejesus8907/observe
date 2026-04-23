import { useCallback, useEffect, useRef, useState } from "react";

type Status = "idle" | "loading" | "ok" | "error";

interface UseAutoRefreshResult<T> {
  data: T | null;
  status: Status;
  error: string | null;
  lastRefreshed: Date | null;
  refresh: () => void;
}

/**
 * Fetches data on mount and at a configurable interval.
 * Returns status + data + a manual refresh trigger.
 */
export function useAutoRefresh<T>(
  fetcher: () => Promise<T>,
  intervalMs = 30_000,
): UseAutoRefreshResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);
  const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const run = useCallback(() => {
    setStatus("loading");
    fetcherRef
      .current()
      .then((result) => {
        setData(result);
        setStatus("ok");
        setError(null);
        setLastRefreshed(new Date());
      })
      .catch((err: unknown) => {
        setStatus("error");
        setError(err instanceof Error ? err.message : String(err));
      });
  }, []);

  useEffect(() => {
    run();
    if (intervalMs > 0) {
      const id = setInterval(run, intervalMs);
      return () => clearInterval(id);
    }
    return undefined;
  }, [run, intervalMs]);

  return { data, status, error, lastRefreshed, refresh: run };
}
