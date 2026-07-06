import { useCallback, useEffect, useRef } from "react";
import { useStore } from "../store";
import { disconnect } from "../api/connection";
import { startScan, getScanResults } from "../api/scan";
import { createScanSocket } from "../api/websocket";
import SummaryCards from "./charts/SummaryCards";
import TypeDistribution from "./charts/TypeDistribution";
import TTLDistribution from "./charts/TTLDistribution";
import KeyGroupBreakdown from "./charts/KeyGroupBreakdown";
import NamespaceTreemap from "./charts/NamespaceTreemap";
import PatternEditor from "./PatternEditor";

export default function Dashboard() {
  const connectionInfo = useStore((s) => s.connectionInfo);
  const scanProgress = useStore((s) => s.scanProgress);
  const setScanProgress = useStore((s) => s.setScanProgress);
  const setScanResult = useStore((s) => s.setScanResult);
  const setDisconnected = useStore((s) => s.setDisconnected);
  const wsRef = useRef<WebSocket | null>(null);

  const handleScan = useCallback(async () => {
    wsRef.current?.close();
    wsRef.current = createScanSocket(
      (progress) => setScanProgress(progress),
      async () => {
        const results = await getScanResults();
        setScanResult(results);
      }
    );
    await startScan();
  }, [setScanProgress, setScanResult]);

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
          {" · "}
          {connectionInfo?.used_memory_human} memory
        </div>
        <div className="flex gap-3">
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
        <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2.5">
          <div
            className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
            style={{ width: `${scanProgress.percent}%` }}
          />
        </div>
      )}

      <SummaryCards />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <TypeDistribution />
        <TTLDistribution />
        <KeyGroupBreakdown />
        <NamespaceTreemap />
      </div>

      <PatternEditor />
    </div>
  );
}
