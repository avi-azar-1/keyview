import { post, del, get } from "./client";
import type { ConnectionInfo } from "../store";

export interface ConnectParams {
  host: string;
  port: number;
  username?: string;
  password?: string;
  db: number;
  cluster_mode?: boolean | null;
}

export function connect(params: ConnectParams) {
  return post<ConnectionInfo>("/api/connect", params);
}

export function disconnect() {
  return del<{ status: string }>("/api/disconnect");
}

export function getConnectionInfo() {
  return get<ConnectionInfo>("/api/connection/info");
}
