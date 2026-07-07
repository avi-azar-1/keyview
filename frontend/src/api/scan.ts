import { post, get } from "./client";
import type { ScanProgress, ScanResult } from "../store";

export function startScan(scanCount?: number) {
  return post<{ status: string }>("/api/scan/start", scanCount ? { scan_count: scanCount } : undefined);
}

export function getScanStatus() {
  return get<ScanProgress>("/api/scan/status");
}

export function getScanResults() {
  return get<ScanResult>("/api/scan/results");
}
