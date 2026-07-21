import type { ScanProgress } from "../store";

export function createDetailScanSocket(
  onProgress: (progress: ScanProgress) => void,
  onComplete: () => void
): WebSocket {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws/scan/detail`);

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.type === "ping") return;
    const data: ScanProgress = msg;
    onProgress(data);
    if (data.status === "completed") {
      onComplete();
    }
  };

  ws.onerror = (e) => {
    console.error("Detail WS error", e);
  };

  return ws;
}
