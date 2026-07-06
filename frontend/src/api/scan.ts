import { post, get } from "./client";
import type { ScanProgress, ScanResult } from "../store";

export function startScan() {
  return post<{ status: string }>("/api/scan/start");
}

export function getScanStatus() {
  return get<ScanProgress>("/api/scan/status");
}

export function getScanResults() {
  return get<ScanResult>("/api/scan/results");
}
