import { useCallback, useEffect, useRef, useState } from "react";
import { useStore } from "../store";
import { disconnect } from "../api/connection";
import { startScan, getScanResults } from "../api/scan";
import { createScanSocket } from "../api/websocket";
import SummaryCards from "./charts/SummaryCards";
import TypeDistribution from "./charts/TypeDistribution";
import TTLDistribution from "./charts/TTLDistribution";
import KeyGroupBreakdown from "./charts/KeyGroupBreakdown";
import NamespaceTreemap from "./charts/NamespaceTreemap";
import PrefixSuggestions from "./PrefixSuggestions";
import PatternEditor from "./PatternEditor";

export default function Dashboard() {
  const connectionInfo = useStore((s) => s.connectionInfo);
  const scanProgress = useStore((s) => s.scanProgress);
  const setScanProgress = useStore((s) => s.setScanProgress);
  const setScanResult = useStore((s) => s.setScanResult);
  const setDisconnected = useStore((s) => s.setDisconnected);
  const wsRef = useRef<WebSocket | null>(null);
  const [scanCount, setScanCount] = useState(10000);

  const handleScan = useCallback(async () => {
    wsRef.current?.close();
    wsRef.current = createScanSocket(
      (progress) => setScanProgress(progress),
      async () => {
        const results = await getScanResults();
        setScanResult(results);
      }
    );
    await startScan(scanCount);
  }, [setScanProgress, setScanResult, scanCount]);

  useEffect(() => {
    handleScan();
    return () => {
      wsRef.current?.close();
    };
  }, [handleScan]);

  async function handleDisconnect() {
    wsRef.current?.close();
    await disconnect();
    setDisconnected();
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="text-sm text-gray-600 dark:text-gray-400">
          Connected to{" "}
          <span className="font-mono font-medium text-gray-900 dark:text-white">
            Redis {connectionInfo?.redis_version}
          </span>
          {connectionInfo?.cluster_mode && (
            <span className="ml-2 px-2 py-0.5 text-xs font-medium bg-purple-100 dark:bg-purple-900 text-purple-700 dark:text-purple-300 rounded-full">
              Cluster · {connectionInfo.node_count} nodes
            </span>
          )}
          {" · "}
          {connectionInfo?.used_memory_human} memory
        </div>
        <div className="flex gap-3 items-center">
          <label className="text-sm text-gray-600 dark:text-gray-400 flex items-center gap-1">
            Batch size
            <input
              type="number"
              min={100}
              step={1000}
              value={scanCount}
              onChange={(e) => setScanCount(parseInt(e.target.value) || 1000)}
              className="w-24 px-2 py-1.5 border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-white text-sm rounded-lg"
            />
          </label>
          <button
            onClick={handleScan}
            disabled={scanProgress.status === "scanning"}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white text-sm font-medium rounded-lg transition-colors"
          >
            {scanProgress.status === "scanning" ? "Scanning..." : "Re-scan"}
          </button>
          <button
            onClick={handleDisconnect}
            className="px-4 py-2 border border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 text-sm font-medium rounded-lg transition-colors"
          >
            Disconnect
          </button>
        </div>
      </div>

      {scanProgress.status === "scanning" && (
        <div className="flex items-center gap-3">
          <div className="flex-1 bg-gray-200 dark:bg-gray-700 rounded-full h-2.5">
            <div
              className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
              style={{ width: `${scanProgress.percent}%` }}
            />
          </div>
          <span className="text-sm font-medium text-gray-700 dark:text-gray-300 w-12 text-right">
            {scanProgress.percent}%
          </span>
        </div>
      )}

      <SummaryCards />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <TypeDistribution />
        <TTLDistribution />
        <KeyGroupBreakdown />
        <NamespaceTreemap />
      </div>

      <PrefixSuggestions />
      <PatternEditor />
    </div>
  );
}
