import React, { createContext, useCallback, useContext, useState } from "react";

export type TimeRange = "15m" | "1h" | "6h" | "24h" | "7d" | "30d";

interface QueryState {
  timeRange: TimeRange;
  service: string;
  severity: string;
  search: string;
}

interface QueryContextValue extends QueryState {
  setTimeRange: (v: TimeRange) => void;
  setService: (v: string) => void;
  setSeverity: (v: string) => void;
  setSearch: (v: string) => void;
  reset: () => void;
}

const defaults: QueryState = {
  timeRange: "1h",
  service: "",
  severity: "",
  search: "",
};

const QueryContext = createContext<QueryContextValue>({
  ...defaults,
  setTimeRange: () => {},
  setService: () => {},
  setSeverity: () => {},
  setSearch: () => {},
  reset: () => {},
});

export function QueryProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<QueryState>(defaults);

  const setTimeRange = useCallback((v: TimeRange) => setState((s) => ({ ...s, timeRange: v })), []);
  const setService = useCallback((v: string) => setState((s) => ({ ...s, service: v })), []);
  const setSeverity = useCallback((v: string) => setState((s) => ({ ...s, severity: v })), []);
  const setSearch = useCallback((v: string) => setState((s) => ({ ...s, search: v })), []);
  const reset = useCallback(() => setState(defaults), []);

  return (
    <QueryContext.Provider value={{ ...state, setTimeRange, setService, setSeverity, setSearch, reset }}>
      {children}
    </QueryContext.Provider>
  );
}

export function useQuery() {
  return useContext(QueryContext);
}
