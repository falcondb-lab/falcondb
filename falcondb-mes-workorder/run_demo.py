"""Full MES demo: Scenario A + B + C"""
import urllib.request, json, time, os, signal, subprocess, sys

API = "http://127.0.0.1:8000"

def post(path, body=None):
    data = json.dumps(body).encode() if body else b"{}"
    req = urllib.request.Request(f"{API}{path}", data=data,
                                headers={"Content-Type": "application/json"}, method="POST")
    try:
        r = urllib.request.urlopen(req)
        return json.loads(r.read()), r.status
    except urllib.error.HTTPError as e:
        return json.loads(e.read()), e.code

def get(path):
    r = urllib.request.urlopen(f"{API}{path}")
    return json.loads(r.read())

# ═══════════════════════════════════════════════════════════════
# SCENARIO A: Normal Production
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("SCENARIO A: Normal Production")
print("=" * 60)

wo, code = post("/api/work-orders", {
    "product_code": "MOTOR-A100", "planned_qty": 1000,
    "operations": ["cutting", "welding", "assembly", "inspection"]
})
print(f"  Create WO: {code} -> id={wo['work_order_id']}, status={wo['status']}")

ops = get(f"/api/work-orders/{wo['work_order_id']}/operations")
print(f"  Operations: {len(ops)}")

for op in ops:
    oid = op["operation_id"]
    r, c = post(f"/api/operations/{oid}/start")
    if c != 200:
        print(f"  ERROR start op#{oid}: {c} {r}")
        continue
    for b in range(1, 5):
        rpt, rc = post(f"/api/operations/{oid}/report",
                       {"report_qty": 250, "reported_by": f"op_{b}"})
        if rc != 200:
            print(f"  ERROR report op#{oid}: {rc} {rpt}")
    done, dc = post(f"/api/operations/{oid}/complete")
    print(f"  [{op['operation_name']}] 4x250 reported, DONE ({dc})")

cwo, cc = post(f"/api/work-orders/{wo['work_order_id']}/complete")
print(f"  Complete WO: {cc} -> status={cwo.get('status', cwo)}")

final = get(f"/api/work-orders/{wo['work_order_id']}")
print(f"  Final: status={final['status']}, completed_qty={final['completed_qty']}")

v = get(f"/api/work-orders/{wo['work_order_id']}/verify")
print(f"  Verify: {v['overall']}")
for inv in v["invariants"]:
    mark = "PASS" if inv["passed"] else "FAIL"
    print(f"    {inv['name']}: {mark} - {inv['detail']}")

wo1_id = wo["work_order_id"]

# ═══════════════════════════════════════════════════════════════
# SCENARIO B: Failover During Production
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("SCENARIO B: Failover During Production")
print("=" * 60)

wo2, _ = post("/api/work-orders", {
    "product_code": "PUMP-B200", "planned_qty": 500,
    "operations": ["machining", "coating", "testing"]
})
wo2_id = wo2["work_order_id"]
print(f"  Create WO#{wo2_id}: {wo2['product_code']}")

ops2 = get(f"/api/work-orders/{wo2_id}/operations")
# Start and partially report first operation
op_first = ops2[0]
post(f"/api/operations/{op_first['operation_id']}/start")
post(f"/api/operations/{op_first['operation_id']}/report",
     {"report_qty": 100, "reported_by": "shift_lead"})
post(f"/api/operations/{op_first['operation_id']}/report",
     {"report_qty": 150, "reported_by": "shift_lead"})
print(f"  [{op_first['operation_name']}] started, 2 reports (100+150=250)")

# Record pre-crash state
pre_crash = get(f"/api/work-orders/{wo2_id}")
pre_reports = get(f"/api/work-orders/{wo2_id}/reports")
print(f"  Pre-crash: status={pre_crash['status']}, completed_qty={pre_crash['completed_qty']}, reports={len(pre_reports)}")

# Save state for later comparison
output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
with open(os.path.join(output_dir, "pre_crash_state.json"), "w") as f:
    json.dump({"work_order": pre_crash, "reports": pre_reports}, f, indent=2)

# Kill FalconDB (simulating crash)
pid_file = os.path.join(output_dir, "falcon.pid")
falcon_pid = int(open(pid_file).read().strip())
print(f"  Killing FalconDB (PID {falcon_pid})...")
try:
    os.kill(falcon_pid, signal.SIGTERM)
except Exception:
    subprocess.run(["taskkill", "/F", "/PID", str(falcon_pid)], capture_output=True)
time.sleep(2)

# Restart FalconDB
print("  Restarting FalconDB...")
falcon_bin = r"H:\falcondb-main\target\release\falcon.exe"
falcon_conf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf", "falcon_mes.toml")
p = subprocess.Popen(
    [falcon_bin, "-c", falcon_conf],
    stdout=open(os.path.join(output_dir, "falcon_recover.log"), "w"),
    stderr=open(os.path.join(output_dir, "falcon_recover_err.log"), "w"),
)
with open(pid_file, "w") as f:
    f.write(str(p.pid))

# Wait for FalconDB to be ready
psql = r"C:\Program Files\PostgreSQL\16\bin\psql.exe"
for i in range(20):
    try:
        result = subprocess.run(
            [psql, "-h", "127.0.0.1", "-p", "5433", "-U", "falcon", "-d", "mes_prod", "-t", "-A", "-c", "SELECT 1;"],
            capture_output=True, text=True, timeout=3
        )
        if result.stdout.strip() == "1":
            print(f"  FalconDB recovered ({i+1}s)")
            break
    except Exception:
        pass
    time.sleep(1)

# Restart backend
backend_pid_file = os.path.join(output_dir, "backend.pid")
try:
    old_bp = int(open(backend_pid_file).read().strip())
    os.kill(old_bp, signal.SIGTERM)
except Exception:
    pass
time.sleep(1)

py_exe = sys.executable
backend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend")
bp = subprocess.Popen(
    [py_exe, os.path.join(backend_dir, "app.py")],
    cwd=backend_dir,
    stdout=open(os.path.join(output_dir, "backend_recover.log"), "w"),
    stderr=open(os.path.join(output_dir, "backend_recover_err.log"), "w"),
    env={**os.environ, "FALCON_HOST": "127.0.0.1", "FALCON_PORT": "5433", "FALCON_DB": "mes_prod"},
)
with open(backend_pid_file, "w") as f:
    f.write(str(bp.pid))

for i in range(15):
    try:
        get("/api/health")
        print(f"  Backend recovered ({i+1}s)")
        break
    except Exception:
        time.sleep(1)

# Compare post-recovery state
post_crash = get(f"/api/work-orders/{wo2_id}")
post_reports = get(f"/api/work-orders/{wo2_id}/reports")
print(f"  Post-recovery: status={post_crash['status']}, completed_qty={post_crash['completed_qty']}, reports={len(post_reports)}")

match_status = pre_crash["status"] == post_crash["status"]
match_qty = pre_crash["completed_qty"] == post_crash["completed_qty"]
match_reports = len(pre_reports) == len(post_reports)
b_pass = match_status and match_qty and match_reports
print(f"  Status match: {match_status}, Qty match: {match_qty}, Reports match: {match_reports}")
print(f"  SCENARIO B: {'PASS' if b_pass else 'FAIL'}")

# Verify both WOs
for wid in [wo1_id, wo2_id]:
    v = get(f"/api/work-orders/{wid}/verify")
    print(f"  WO#{wid} verify: {v['overall']}")

# ═══════════════════════════════════════════════════════════════
# SCENARIO C: Concurrent Reporting
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("SCENARIO C: Concurrent Reporting")
print("=" * 60)

wo3, _ = post("/api/work-orders", {
    "product_code": "VALVE-C300", "planned_qty": 200,
    "operations": ["forging", "machining"]
})
wo3_id = wo3["work_order_id"]
print(f"  Create WO#{wo3_id}: {wo3['product_code']}")

ops3 = get(f"/api/work-orders/{wo3_id}/operations")
op_forge = ops3[0]
post(f"/api/operations/{op_forge['operation_id']}/start")

# Sequential reports from multiple "operators"
total_reported = 0
for i in range(1, 11):
    qty = 20
    rpt, rc = post(f"/api/operations/{op_forge['operation_id']}/report",
                   {"report_qty": qty, "reported_by": f"worker_{i}"})
    if rc == 200:
        total_reported += qty

print(f"  10 reports of 20 each, total expected: {total_reported}")

wo3_final = get(f"/api/work-orders/{wo3_id}")
print(f"  DB completed_qty: {wo3_final['completed_qty']}")
c_pass = wo3_final["completed_qty"] == total_reported
print(f"  SCENARIO C: {'PASS' if c_pass else 'FAIL'}")

# ═══════════════════════════════════════════════════════════════
# Final Report
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 60)
print("FINAL VERIFICATION REPORT")
print("=" * 60)

all_pass = True
wos = get("/api/work-orders")
for w in wos:
    wid = w["work_order_id"]
    v = get(f"/api/work-orders/{wid}/verify")
    status = v["overall"]
    if status != "PASS":
        all_pass = False
    print(f"  WO#{wid} ({w['product_code']}): status={w['status']}, qty={w['completed_qty']}, verify={status}")
    for inv in v["invariants"]:
        mark = "PASS" if inv["passed"] else "FAIL"
        print(f"    {inv['name']}: {mark}")

print()
a_ok = final["status"] == "COMPLETED" and final["completed_qty"] == 4000
overall = "PASS" if (a_ok and b_pass and c_pass and all_pass) else "FAIL"
print(f"  Scenario A (normal production):     {'PASS' if a_ok else 'FAIL'}")
print(f"  Scenario B (failover recovery):     {'PASS' if b_pass else 'FAIL'}")
print(f"  Scenario C (concurrent reporting):  {'PASS' if c_pass else 'FAIL'}")
print(f"  All invariants:                     {'PASS' if all_pass else 'FAIL'}")
print(f"  ===================================")
print(f"  OVERALL: {overall}")

# Save report
report_path = os.path.join(output_dir, "verification_report.txt")
with open(report_path, "w") as f:
    f.write(f"FalconDB MES Workorder PoC — Verification Report\n")
    f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    f.write(f"Scenario A (normal production):     {'PASS' if a_ok else 'FAIL'}\n")
    f.write(f"Scenario B (failover recovery):     {'PASS' if b_pass else 'FAIL'}\n")
    f.write(f"Scenario C (concurrent reporting):  {'PASS' if c_pass else 'FAIL'}\n")
    f.write(f"All invariants:                     {'PASS' if all_pass else 'FAIL'}\n")
    f.write(f"OVERALL: {overall}\n")
print(f"\nReport saved to {report_path}")
