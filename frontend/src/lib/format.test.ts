import { describe, expect, it } from "vitest";
import { fmtBytes, fmtDuration, fmtInt, fmtMs, fmtNum, fmtPct } from "./format";

describe("format", () => {
  it("renders missing values as an en dash", () => {
    for (const f of [fmtInt, fmtNum, fmtMs, fmtBytes, fmtPct]) expect(f(null)).toBe("–");
    expect(fmtNum(Number.NaN)).toBe("–");
    expect(fmtMs(-1)).toBe("–");
  });

  it("formats integers with thousands separators", () => {
    expect(fmtInt(2870.4)).toBe("2,870");
    expect(fmtInt(0)).toBe("0");
  });

  it("picks a latency unit by magnitude", () => {
    expect(fmtMs(0.5)).toBe("0.50 ms");
    expect(fmtMs(21.9)).toBe("22 ms");
    expect(fmtMs(1500)).toBe("1.50 s");
    expect(fmtMs(12_000)).toBe("12.0 s");
  });

  it("formats bytes in binary units", () => {
    expect(fmtBytes(512)).toBe("512 B");
    expect(fmtBytes(306_914)).toBe("299.7 KB");
    expect(fmtBytes(16 * 1024 * 1024)).toBe("16.0 MB");
  });

  it("formats durations and percentages", () => {
    expect(fmtDuration(59_999)).toBe("59s");
    expect(fmtDuration(61_000)).toBe("1m 1s");
    expect(fmtDuration(3_660_000)).toBe("1h 1m");
    expect(fmtPct(0.826)).toBe("82.6%");
  });
});
