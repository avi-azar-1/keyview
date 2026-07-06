import type { ScanProgress } from "../store";

export function createScanSocket(
  onProgress: (progress: ScanProgress) => void,
  onComplete: () => void
): WebSocket {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws/scan`);

  ws.onmessage = (event) => {
    const data: ScanProgress = JSON.parse(event.data);
    onProgress(data);
    if (data.status === "completed") {
      onComplete();
    }
  };

  return ws;
}
