import { useCallback, useEffect, useRef, useState } from "react";
import { useStore } from "../store";
import { disconnect } from "../api/connection";
import { startScan, getScanResults } from "../api/scan";
import { createDetailScanSocket } from "../api/websocket";
import SummaryCards from "./charts/SummaryCards";
import TypeDistribution from "./charts/TypeDistribution";
import TTLDistribution from "./charts/TTLDistribution";
import KeyGroupBreakdown from "./charts/KeyGroupBreakdown";
import NamespaceTreemap from "./charts/NamespaceTreemap";
import PrefixSuggestions from "./PrefixSuggestions";
import PatternEditor from "./PatternEditor";
import type { ScanProgress } from "../store";

export default function Dashboard() {
  const connectionInfo = useStore((s) => s.connectionInfo);
  const scanProgress = useStore((s) => s.scanProgress);
  const detailProgress = useStore((s) => s.detailProgress);
  const setScanProgress = useStore((s) => s.setScanProgress);
  const setDetailProgress = useStore((s) => s.setDetailProgress);
  const setScanResult = useStore((s) => s.setScanResult);
  const updateDetailResult = useStore((s) => s.updateDetailResult);
  const setDisconnected = useStore((s) => s.setDisconnected);
  const scanWsRef = useRef<WebSocket | null>(null);
  const detailWsRef = useRef<WebSocket | null>(null);
  const [scanCount, setScanCount] = useState(10000);
  const scanStartRef = useRef<number | null>(null);
  const detailStartRef = useRef<number | null>(null);
  const [eta, setEta] = useState<string>("");
  const [detailEta, setDetailEta] = useState<string>("");
  const pendingScanRef = useRef(false);

  // Persistent main scan websocket
  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/scan`);
    ws.onmessage = (event) => {
      const data: ScanProgress = JSON.parse(event.data);
      setScanProgress(data);
      if (data.status === "scanning" && !scanStartRef.current) {
        scanStartRef.current = Date.now();
      }
      if (data.status === "scanning" && data.percent > 0 && scanStartRef.current) {
        const elapsed = (Date.now() - scanStartRef.current) / 1000;
        const remaining = (elapsed / data.percent) * (100 - data.percent);
        if (remaining < 60) {
          setEta(`~${Math.ceil(remaining)}s left`);
        } else {
          setEta(`~${Math.ceil(remaining / 60)}m left`);
        }
      }
      if (data.status === "completed") {
        setEta("");
        scanStartRef.current = null;
        if (pendingScanRef.current) {
          pendingScanRef.current = false;
          getScanResults().then((results) => {
            setScanResult(results);
            // Start listening for detail phase
            detailStartRef.current = Date.now();
            detailWsRef.current?.close();
            detailWsRef.current = createDetailScanSocket(
              (progress) => {
                setDetailProgress(progress);
                if (progress.status === "scanning" && progress.percent > 0 && detailStartRef.current) {
                  const elapsed = (Date.now() - detailStartRef.current) / 1000;
                  const remaining = (elapsed / progress.percent) * (100 - progress.percent);
                  if (remaining < 60) {
                    setDetailEta(`~${Math.ceil(remaining)}s left`);
                  } else {
                    setDetailEta(`~${Math.ceil(remaining / 60)}m left`);
                  }
                }
              },
              async () => {
                setDetailEta("");
                const r = await getScanResults();
                updateDetailResult(r.type_counts, r.ttl_buckets);
              }
            );
          });
        }
      }
    };
    scanWsRef.current = ws;
    return () => {
      ws.close();
      scanWsRef.current = null;
    };
  }, [setScanProgress, setScanResult, setDetailProgress, updateDetailResult]);

  const handleScan = useCallback(async () => {
    scanStartRef.current = Date.now();
    setEta("");
    setDetailEta("");
    pendingScanRef.current = true;
    detailWsRef.current?.close();
    await startScan(scanCount);
  }, [scanCount]);

  useEffect(() => {
    handleScan();
  }, [handleScan]);

  async function handleDisconnect() {
    scanWsRef.current?.close();
    scanWsRef.current = null;
    detailWsRef.current?.close();
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
          <span className="text-sm font-medium text-gray-700 dark:text-gray-300 text-right whitespace-nowrap">
            {scanProgress.percent}%{eta && ` · ${eta}`}
          </span>
        </div>
      )}

      <SummaryCards />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <KeyGroupBreakdown />
        <NamespaceTreemap />
      </div>

      <PrefixSuggestions />
      <PatternEditor />

      {/* Phase 2: Type + TTL (background) */}
      <div className="border-t border-gray-200 dark:border-gray-700 pt-6">
        <div className="flex items-center gap-3 mb-4">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
            Key Types & TTL
          </h3>
          {detailProgress.status === "scanning" && (
            <span className="text-xs font-medium text-gray-500 dark:text-gray-400">
              Analyzing...
            </span>
          )}
          {detailProgress.status === "completed" && (
            <span className="text-xs font-medium text-green-600 dark:text-green-400">
              Done
            </span>
          )}
        </div>

        {detailProgress.status === "scanning" && (
          <div className="flex items-center gap-3 mb-4">
            <div className="flex-1 bg-gray-200 dark:bg-gray-700 rounded-full h-2">
              <div
                className="bg-purple-600 h-2 rounded-full transition-all duration-300"
                style={{ width: `${detailProgress.percent}%` }}
              />
            </div>
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300 text-right whitespace-nowrap">
              {detailProgress.percent}%{detailEta && ` · ${detailEta}`}
            </span>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <TypeDistribution />
          <TTLDistribution />
        </div>
      </div>
    </div>
  );
}
