import type { ApiErrorBody } from "./types";
import { getSettings } from "../lib/settings";

export class ApiError extends Error {
  constructor(public status: number, public body: ApiErrorBody | null, message: string) {
    super(message);
  }
  get code() {
    return this.body?.error ?? `HTTP_${this.status}`;
  }
}

/** API base for cluster-wide calls (the pinned node, or any node via the proxy) or one specific node. */
export function base(node?: string): string {
  const pinned = node ?? getSettings().pinnedNode;
  return pinned ? `/nodes/${pinned}/api/v1` : "/api/v1";
}

export interface RequestOptions {
  method?: string;
  body?: unknown;
  node?: string;
  admin?: boolean;
  headers?: Record<string, string>;
  signal?: AbortSignal;
}

export async function api<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json", ...opts.headers };
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  if (opts.admin) {
    const token = getSettings().adminToken;
    if (token) headers["Authorization"] = `Bearer ${token}`;
  }
  let res: Response;
  try {
    res = await fetch(base(opts.node) + path, {
      method: opts.method ?? "GET",
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      signal: opts.signal,
    });
  } catch (e) {
    if ((e as Error).name === "AbortError") throw e;
    throw new ApiError(0, null, "Cannot reach the QuorumDB API. Is the cluster running?");
  }
  const text = await res.text();
  let data: unknown = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = null;
  }
  if (!res.ok) {
    const body = (data as ApiErrorBody) ?? null;
    throw new ApiError(res.status, body, body?.message ?? `Request failed with HTTP ${res.status}`);
  }
  return data as T;
}

/** Raw request for the operations console: returns status, headers and body without throwing. */
export async function rawRequest(method: string, path: string, body?: string, node?: string) {
  const started = performance.now();
  const res = await fetch(base(node) + path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body,
  });
  const text = await res.text();
  const headers: Record<string, string> = {};
  res.headers.forEach((v, k) => {
    if (k.startsWith("x-quorumdb")) headers[k] = v;
  });
  return { status: res.status, headers, body: text, ms: performance.now() - started };
}
