import { post, get } from "./client";
import type { ScanProgress, ScanResult } from "../store";

export function startScan(scanCount?: number, estimatePercent?: number) {
  const body: { scan_count?: number; estimate_percent?: number } = {};
  if (scanCount) body.scan_count = scanCount;
  if (estimatePercent != null) body.estimate_percent = estimatePercent;
  return post<{ status: string }>(
    "/api/scan/start",
    Object.keys(body).length ? body : undefined
  );
}

export function getScanStatus() {
  return get<ScanProgress>("/api/scan/status");
}

export function getScanResults() {
  return get<ScanResult>("/api/scan/results");
}
