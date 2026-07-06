import { get, post, del } from "./client";
import type { Pattern } from "../store";

export function listPatterns() {
  return get<Pattern[]>("/api/patterns");
}

export function addPattern(pattern: string) {
  return post<Pattern>("/api/patterns", { pattern });
}

export function deletePattern(id: string) {
  return del<{ status: string }>(`/api/patterns/${id}`);
}

export function applyPatterns() {
  return post<{ pattern_counts: Record<string, number> }>("/api/patterns/apply");
}
