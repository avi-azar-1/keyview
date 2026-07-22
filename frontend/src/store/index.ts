import { create } from "zustand";

export interface ConnectionInfo {
  redis_version: string;
  connected_clients: number;
  used_memory_human: string;
  total_keys: number;
  uptime_in_seconds: number;
  cluster_mode: boolean;
  node_count: number;
}

export interface TTLBucket {
  label: string;
  count: number;
}

export interface PrefixSuggestion {
  prefix: string;
  key_count: number;
  depth: number;
  child_count: number;
  coverage_pct: number;
}

export interface NamespaceBreakdown {
  namespace: string;
  total: number;
  type_counts: Record<string, number>;
  ttl_buckets: TTLBucket[];
}

export interface ScanResult {
  total_keys: number;
  type_counts: Record<string, number>;
  ttl_buckets: TTLBucket[];
  namespace_counts: Record<string, number>;
  pattern_counts: Record<string, number>;
  suggested_prefixes: PrefixSuggestion[];
  namespace_breakdowns: NamespaceBreakdown[];
}

export interface ScanProgress {
  status: "idle" | "scanning" | "completed";
  scanned: number;
  total_estimate: number;
  percent: number;
}

export interface Pattern {
  id: string;
  pattern: string;
}

interface AppState {
  connected: boolean;
  connectionInfo: ConnectionInfo | null;
  scanProgress: ScanProgress;
  detailProgress: ScanProgress;
  scanResult: ScanResult | null;
  patterns: Pattern[];
  darkMode: boolean;
  selectedNamespace: string;

  setConnected: (info: ConnectionInfo) => void;
  setDisconnected: () => void;
  setScanProgress: (progress: ScanProgress) => void;
  setDetailProgress: (progress: ScanProgress) => void;
  setScanResult: (result: ScanResult) => void;
  setPatterns: (patterns: Pattern[]) => void;
  updatePatternCounts: (counts: Record<string, number>) => void;
  updateDetailResult: (
    type_counts: Record<string, number>,
    ttl_buckets: TTLBucket[],
    namespace_breakdowns: NamespaceBreakdown[]
  ) => void;
  setSelectedNamespace: (namespace: string) => void;
  toggleDarkMode: () => void;
}

export const useStore = create<AppState>((set) => ({
  connected: false,
  connectionInfo: null,
  scanProgress: { status: "idle", scanned: 0, total_estimate: 0, percent: 0 },
  detailProgress: { status: "idle", scanned: 0, total_estimate: 0, percent: 0 },
  scanResult: null,
  patterns: [],
  darkMode: window.matchMedia("(prefers-color-scheme: dark)").matches,
  selectedNamespace: "All",

  setConnected: (info) => set({ connected: true, connectionInfo: info }),
  setDisconnected: () =>
    set({
      connected: false,
      connectionInfo: null,
      scanResult: null,
      selectedNamespace: "All",
      scanProgress: { status: "idle", scanned: 0, total_estimate: 0, percent: 0 },
      detailProgress: { status: "idle", scanned: 0, total_estimate: 0, percent: 0 },
    }),
  setScanProgress: (progress) => set({ scanProgress: progress }),
  setDetailProgress: (progress) => set({ detailProgress: progress }),
  setScanResult: (result) => set({ scanResult: result, selectedNamespace: "All" }),
  setPatterns: (patterns) => set({ patterns }),
  updatePatternCounts: (counts) =>
    set((state) => ({
      scanResult: state.scanResult
        ? { ...state.scanResult, pattern_counts: counts }
        : null,
    })),
  updateDetailResult: (type_counts, ttl_buckets, namespace_breakdowns) =>
    set((state) => ({
      scanResult: state.scanResult
        ? { ...state.scanResult, type_counts, ttl_buckets, namespace_breakdowns }
        : null,
    })),
  setSelectedNamespace: (namespace) => set({ selectedNamespace: namespace }),
  toggleDarkMode: () =>
    set((state) => {
      const next = !state.darkMode;
      document.documentElement.classList.toggle("dark", next);
      return { darkMode: next };
    }),
}));
