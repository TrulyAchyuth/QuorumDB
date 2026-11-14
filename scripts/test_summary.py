#!/usr/bin/env python3
"""Summarise JUnit XML results of every Gradle module into Markdown.

Usage: scripts/test_summary.py [output.md]
Reads */build/test-results/test/*.xml, i.e. it reports what was actually executed.
"""
import glob
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(root, "docs", "evidence", "test-summary.md")

modules = {}
for path in sorted(glob.glob(os.path.join(root, "*", "build", "test-results", "*", "*.xml"))):
    parts = os.path.relpath(path, root).split(os.sep)
    module = parts[0] if parts[3] == "test" else parts[0] + " (" + parts[3] + ")"
    suite = ET.parse(path).getroot()
    cases = []
    for case in suite.findall("testcase"):
        status = "passed"
        if case.find("failure") is not None or case.find("error") is not None:
            status = "FAILED"
        elif case.find("skipped") is not None:
            status = "skipped"
        cases.append((case.get("name"), status, float(case.get("time", "0"))))
    sysout = suite.findtext("system-out") or ""
    jqwik = [line.strip() for line in sysout.splitlines()
             if line.strip().startswith(("tries =", "checks =", "seed =", "EVIDENCE"))]
    modules.setdefault(module, []).append({
        "name": suite.get("name"), "tests": int(suite.get("tests")), "skipped": int(suite.get("skipped")),
        "failures": int(suite.get("failures")) + int(suite.get("errors")), "time": float(suite.get("time")),
        "timestamp": suite.get("timestamp"), "cases": cases, "jqwik": jqwik})

total = sum(s["tests"] for m in modules.values() for s in m)
failed = sum(s["failures"] for m in modules.values() for s in m)
skipped = sum(s["skipped"] for m in modules.values() for s in m)
passed = total - failed - skipped

lines = ["# Test results", "",
         f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} by `scripts/test_summary.py` "
         "from the JUnit XML reports of the last `./gradlew clean test integrationTest` run.", "",
         f"**Total: {total} tests, {passed} passed, {failed} failed, {skipped} skipped.**", "",
         "| Module | Suite | Tests | Passed | Failed | Skipped | Time (s) |", "|---|---|---:|---:|---:|---:|---:|"]
for module, suites in modules.items():
    for s in suites:
        lines.append(f"| {module} | {s['name'].split('.')[-1]} | {s['tests']} | "
                     f"{s['tests'] - s['failures'] - s['skipped']} | {s['failures']} | {s['skipped']} | {s['time']:.2f} |")
lines.append("")
for module, suites in modules.items():
    lines += [f"## {module}", ""]
    for s in suites:
        lines += [f"### {s['name']}", ""]
        for name, status, seconds in s["cases"]:
            lines.append(f"- {status}: `{name}` ({seconds:.3f}s)")
        if s["jqwik"]:
            lines += ["", "Recorded output (jqwik tries/seeds, or EVIDENCE lines with real measurements):", "", "```"]
            lines += s["jqwik"] + ["```"]
        lines.append("")

os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, "w") as f:
    f.write("\n".join(lines))
print(f"{total} tests, {passed} passed, {failed} failed, {skipped} skipped -> {os.path.relpath(out_path, root)}")
sys.exit(1 if failed else 0)
