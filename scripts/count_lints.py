#!/usr/bin/env python3
"""Parse cargo clippy JSON output and summarize lint counts."""
import sys, json

lints = {}
files = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        m = json.loads(line)
    except json.JSONDecodeError:
        continue
    if m.get("reason") != "compiler-message":
        continue
    msg = m.get("message", {})
    if msg.get("level") != "warning":
        continue
    code = msg.get("code")
    if not code:
        continue
    lint = code.get("code", "unknown")
    lints[lint] = lints.get(lint, 0) + 1
    # track which files
    for span in msg.get("spans", []):
        fn = span.get("file_name", "")
        if fn not in files:
            files[fn] = {}
        files[fn][lint] = files[fn].get(lint, 0) + 1

print(f"\n{'COUNT':>6}  LINT")
print("-" * 50)
for lint, count in sorted(lints.items(), key=lambda x: -x[1]):
    print(f"{count:6d}  {lint}")

print(f"\nTotal warnings: {sum(lints.values())}")
print(f"\nTop files:")
file_totals = {f: sum(v.values()) for f, v in files.items()}
for fn, total in sorted(file_totals.items(), key=lambda x: -x[1])[:15]:
    print(f"  {total:4d}  {fn}")
