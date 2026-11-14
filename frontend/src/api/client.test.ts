// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Settings are cached at module level, so every test loads fresh modules.
async function load() {
  vi.resetModules();
  const settings = await import("../lib/settings");
  const client = await import("./client");
  return { ...settings, ...client };
}

/** Awaits a promise that must reject and returns the rejection as an ApiError-shaped value. */
async function failure(p: Promise<unknown>): Promise<{ status: number; code: string; message: string }> {
  try {
    await p;
  } catch (e) {
    return e as { status: number; code: string; message: string };
  }
  throw new Error("expected the call to fail");
}

function respond(status: number, body: unknown) {
  // A Response body can be read once, so every call gets a new one.
  return vi.fn().mockImplementation(async () =>
    new Response(body === undefined ? "" : typeof body === "string" ? body : JSON.stringify(body), { status }),
  );
}

describe("api client", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("routes cluster-wide calls to /api/v1 and pinned or per-node calls to /nodes/<id>", async () => {
    const { base, updateSettings } = await load();
    expect(base()).toBe("/api/v1");
    expect(base("n2")).toBe("/nodes/n2/api/v1");
    updateSettings({ pinnedNode: "n3" });
    expect(base()).toBe("/nodes/n3/api/v1");
  });

  it("returns parsed JSON on success", async () => {
    const { api } = await load();
    const fetchMock = respond(200, { leaderId: "n1", term: 3 });
    vi.stubGlobal("fetch", fetchMock);
    await expect(api("/leader")).resolves.toEqual({ leaderId: "n1", term: 3 });
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/leader");
  });

  it("raises the server's error code and message", async () => {
    const { api, ApiError } = await load();
    vi.stubGlobal("fetch", respond(503, { error: "NO_LEADER", message: "no leader is known", node: "n2", leaderHint: null }));
    const err = await failure(api("/kv/a"));
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(503);
    expect(err.code).toBe("NO_LEADER");
    expect(err.message).toBe("no leader is known");
  });

  it("copes with non-JSON error bodies (e.g. a proxy's 502 page)", async () => {
    const { api } = await load();
    vi.stubGlobal("fetch", respond(502, "<html>Bad Gateway</html>"));
    const err = await failure(api("/cluster"));
    expect(err.code).toBe("HTTP_502");
    expect(err.message).toBe("Request failed with HTTP 502");
  });

  it("reports an unreachable API in plain words", async () => {
    const { api } = await load();
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new TypeError("Failed to fetch")));
    const err = await failure(api("/cluster"));
    expect(err.status).toBe(0);
    expect(err.message).toMatch(/Cannot reach the QuorumDB API/);
  });

  it("sends the admin token only on admin calls, and keeps it in sessionStorage only", async () => {
    const { api, updateSettings } = await load();
    updateSettings({ adminToken: "secret-token" });
    expect(sessionStorage.getItem("quorumdb.adminToken")).toBe("secret-token");
    expect(localStorage.getItem("quorumdb.settings")).not.toContain("secret-token");

    const fetchMock = respond(201, { faultId: "f1" });
    vi.stubGlobal("fetch", fetchMock);
    await api("/faults", { method: "POST", body: { type: "HEAL" }, admin: true });
    await api("/cluster");
    const [adminCall, plainCall] = fetchMock.mock.calls;
    expect(adminCall[1].headers.Authorization).toBe("Bearer secret-token");
    expect(adminCall[1].headers["Content-Type"]).toBe("application/json");
    expect(plainCall[1].headers.Authorization).toBeUndefined();
  });

  it("ignores invalid stored settings", async () => {
    localStorage.setItem("quorumdb.settings", JSON.stringify({ pinnedNode: 7, pollMs: 10 }));
    const { getSettings } = await load();
    expect(getSettings().pinnedNode).toBeNull();
    expect(getSettings().pollMs).toBe(1000);
  });
});
