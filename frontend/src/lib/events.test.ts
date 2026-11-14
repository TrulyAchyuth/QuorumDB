import { describe, expect, it } from "vitest";
import { describe as describeEvent, eventTone, groupOf, label } from "./events";

describe("events", () => {
  it("groups known types and falls back to lifecycle", () => {
    expect(groupOf("LEADER_ELECTED")).toBe("election");
    expect(groupOf("ENTRY_COMMITTED")).toBe("replication");
    expect(groupOf("SNAPSHOT_SENT")).toBe("snapshot");
    expect(groupOf("FAULT_INJECTED")).toBe("fault");
    expect(groupOf("SOMETHING_NEW")).toBe("lifecycle");
  });

  it("turns a type into a sentence-case label", () => {
    expect(label("LEADER_ELECTED")).toBe("Leader elected");
  });

  it("marks failures critical and recoveries good", () => {
    expect(eventTone("NODE_CRASHED")).toBe("critical");
    expect(eventTone("LEADER_ELECTED")).toBe("accent");
    expect(eventTone("NODE_RECOVERED")).toBe("good");
    expect(eventTone("HEARTBEAT_SENT")).toBe("muted");
  });

  it("describes an event from its fields", () => {
    expect(describeEvent({ type: "ENTRY_COMMITTED", term: 2, index: 41, peer: null, detail: null })).toBe("index 41");
    expect(describeEvent({ type: "SNAPSHOT_SENT", term: 2, index: 10002, peer: "n3", detail: null })).toBe("peer n3");
    // The peer of a fault event is its target list, already in the detail.
    expect(describeEvent({ type: "FAULT_INJECTED", term: 0, index: 0, peer: "n2", detail: "ISOLATE [n2]" })).toBe("ISOLATE [n2]");
  });
});
