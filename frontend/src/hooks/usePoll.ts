import { useCallback, useEffect, useRef, useState } from "react";
import { getSettings } from "../lib/settings";

export interface PollState<T> {
  data: T | null;
  error: Error | null;
  loading: boolean; // true only before the first response
  refreshing: boolean;
  refresh: () => void;
}

/**
 * Polls an async loader. Keeps the previous data while refetching (no skeleton flash) and pauses while
 * the tab is hidden. intervalMs = 0 loads once.
 */
export function usePoll<T>(loader: (signal: AbortSignal) => Promise<T>, deps: unknown[], intervalMs?: number): PollState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [tick, setTick] = useState(0);
  const loaderRef = useRef(loader);
  loaderRef.current = loader;
  const interval = intervalMs ?? getSettings().pollMs;

  useEffect(() => {
    setLoading(true);
    setData(null);
    setError(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();
    const run = async () => {
      if (document.hidden && interval > 0) {
        timer = setTimeout(run, interval);
        return;
      }
      setRefreshing(true);
      try {
        const result = await loaderRef.current(controller.signal);
        if (!cancelled) {
          setData(result);
          setError(null);
        }
      } catch (e) {
        if (!cancelled && (e as Error).name !== "AbortError") setError(e as Error);
      } finally {
        if (!cancelled) {
          setLoading(false);
          setRefreshing(false);
          if (interval > 0) timer = setTimeout(run, interval);
        }
      }
    };
    run();
    return () => {
      cancelled = true;
      controller.abort();
      if (timer) clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, interval, tick]);

  const refresh = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, refreshing, refresh };
}
