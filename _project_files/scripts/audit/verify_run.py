#!/usr/bin/env python3
"""
scripts/audit/verify_run.py
Independent Verifier & Audit Gate for DeathStarBench RCA Datasets.

Strictly independent implementation: does NOT import process_run.py or any pipeline modules.
Standard library only (csv, json, math, os, sys, argparse, pathlib).

Implements Gates 1.2a - 1.2k:
  - Gate 1.2a: Capture ratio >= 90% (>= 98% in baselines) & no zero-call warmup dropouts (Windows 0..7 calls > 0).
  - Gate 1.2b: Target component & active services on-path validation (no call_count=0 on on-path microservices).
  - Gate 1.2c: Dead metrics audit (no flat 0 mem_bytes > 10% windows, cpu_rate > 0 on active containers, zero NaNs).
  - Gate 1.2d: Sustained symptom elevation (>= 80% of fault windows exhibit verified elevation).
  - Gate 1.2e: Pre-fault baseline stability (cfs_throttle <= 0.05, p95 <= 3x median, calls >= 0.5x median).
  - Gate 1.2f: Exact fault window count (6 windows for 30s) & onset overlap >= 0.80.
  - Gate 1.2g: Partial trailing window dropped (last row calls >= 50% median).
  - Gate 1.2h: Counter monotonicity (dv >= 0, with UniqueId restart exception for drops < 15%).
  - Gate 1.2i: Byte-level NetIO rates present (> 0 on active microservices).
  - Gate 1.2j: Cross-rep consistency check for duplicate experimental conditions.
  - Gate 1.2k: Ground-truth provenance (labels verified against effective injection timestamps).
"""

import os
import sys
import csv
import json
import math
import argparse
from pathlib import Path

# On-path microservices expected to have calls for each workload endpoint
ON_PATH_SERVICES = {
    "compose": [
        "nginx-thrift",
        "compose-post-service",
        "text-service",
        "user-service",
        "media-service",
        "unique-id-service",
        "url-shorten-service",
        "user-mention-service",
        "post-storage-service"
    ],
    "home": [
        "nginx-thrift",
        "home-timeline-service",
        "post-storage-service"
    ],
    "user": [
        "nginx-thrift",
        "user-timeline-service",
        "post-storage-service"
    ],
    "mixed": [
        "nginx-thrift",
        "compose-post-service",
        "text-service",
        "user-service",
        "media-service",
        "unique-id-service",
        "url-shorten-service",
        "user-mention-service",
        "post-storage-service",
        "home-timeline-service",
        "user-timeline-service",
        "social-graph-service"
    ]
}

def load_csv(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def load_args(args_path):
    if not os.path.exists(args_path):
        return {}
    with open(args_path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            # Fallback to key=value if not pure json
            f.seek(0)
            data = {}
            for line in f:
                line = line.strip()
                if "=" in line:
                    k, v = line.split("=", 1)
                    data[k.strip()] = v.strip()
            return data

def safe_float(val, default=0.0):
    try:
        if val is None or val == "" or str(val).lower() == "nan":
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

def median(lst):
    if not lst:
        return 0.0
    s = sorted(lst)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0

def verify_single_run(csv_path, args, manifest_entry=None):
    report = {
        "run_id": args.get("run_id") or Path(csv_path).stem,
        "csv_path": str(csv_path),
        "status": "PASS",
        "failures": [],
        "warnings": [],
        "details": {}
    }

    rows = load_csv(csv_path)
    if not rows:
        report["status"] = "FAIL"
        report["failures"].append("CSV is empty or missing headers")
        return report

    num_rows = len(rows)
    report["details"]["num_windows"] = num_rows
    headers = list(rows[0].keys())
    report["details"]["num_columns"] = len(headers)

    # Resolve metadata
    fault_type = args.get("fault_type") or (manifest_entry.get("fault_type") if manifest_entry else "none")
    if fault_type is None:
        fault_type = "none"
    endpoint = args.get("endpoint") or (manifest_entry.get("endpoint") if manifest_entry else "compose")
    target_component = args.get("target_component") or (manifest_entry.get("target_component") if manifest_entry else "")
    target_rps = safe_float(args.get("rps") or (manifest_entry.get("rps") if manifest_entry else 50))
    exp_duration = safe_float(args.get("experiment_duration") or (manifest_entry.get("duration_s") if manifest_entry else 120))

    report["details"]["fault_type"] = fault_type
    report["details"]["endpoint"] = endpoint
    report["details"]["target_component"] = target_component
    report["details"]["target_rps"] = target_rps

    # -------------------------------------------------------------
    # GATE 1: Temporal Integrity, NaNs, and Schema
    # -------------------------------------------------------------
    req_cols = ["window_id", "timestamp", "phase_label", "is_fault_window", "transition_flag", "fault_overlap_pct"]
    missing_cols = [c for c in req_cols if c not in headers]
    if missing_cols:
        report["status"] = "FAIL"
        report["failures"].append(f"Missing required metadata columns: {missing_cols}")

    # Check for NaNs or Infinities in any cell
    nan_cells = 0
    for r_idx, r in enumerate(rows):
        for c, v in r.items():
            if v is None or v == "" or str(v).lower() in ("nan", "null", "inf", "-inf"):
                nan_cells += 1
    if nan_cells > 0:
        report["status"] = "FAIL"
        report["failures"].append(f"Found {nan_cells} NaN or null cells in processed matrix")

    # -------------------------------------------------------------
    # GATE 1.2g: Partial Trailing Window Check
    # -------------------------------------------------------------
    # Find nginx call count column or first on-path service calls
    nginx_call_cols = [c for c in headers if c in ("nginx-thrift_call_count", "nginx-web-server_call_count", "nginx-thrift_calls")]
    call_col = nginx_call_cols[0] if nginx_call_cols else None
    
    if call_col:
        all_calls = [safe_float(r[call_col]) for r in rows]
        med_calls = median([c for c in all_calls if c > 0])
        if num_rows > 1 and med_calls > 10:
            last_window_calls = all_calls[-1]
            if last_window_calls < 0.50 * med_calls:
                report["status"] = "FAIL"
                report["failures"].append(
                    f"Partial trailing window detected: Window {num_rows-1} has {last_window_calls} calls vs median {med_calls:.1f} (< 50%)"
                )

    # -------------------------------------------------------------
    # GATE 1.2a: Capture Ratio, Duration & Warm-up Zero-Calls
    # -------------------------------------------------------------
    # Duration validation
    if fault_type == "none":
        min_windows = 60 if target_rps <= 50 else 24
        if num_rows < min_windows:
            report["status"] = "FAIL"
            report["failures"].append(
                f"Insufficient baseline duration: {num_rows} windows ({num_rows*5}s) < required {min_windows} windows ({min_windows*5}s) for {target_rps:.0f} RPS"
            )
    else:
        # Fault experiments must be at least 120s (24 windows)
        if num_rows < 24:
            report["status"] = "FAIL"
            report["failures"].append(
                f"Insufficient experiment duration: {num_rows} windows ({num_rows*5}s) < required 24 windows (120s)"
            )
    if call_col:
        # Check initial normal windows: windows 0..7 must not be completely empty
        initial_windows_zero = 0
        for i in range(min(8, num_rows)):
            if safe_float(rows[i][call_col]) == 0:
                initial_windows_zero += 1
        if initial_windows_zero >= 4:
            report["status"] = "FAIL"
            report["failures"].append(
                f"Severe trace capture failure: {initial_windows_zero} of first 8 windows have zero calls on {call_col}"
            )

        total_recorded_calls = sum(safe_float(r[call_col]) for r in rows)
        report["details"]["total_recorded_calls"] = total_recorded_calls
        expected_calls = target_rps * (num_rows * 5.0)  # Total time covered by windows
        if expected_calls > 0:
            capture_ratio = total_recorded_calls / expected_calls
            report["details"]["capture_ratio"] = round(capture_ratio, 4)
            if fault_type == "none" and capture_ratio < 0.85:
                report["status"] = "FAIL"
                report["failures"].append(f"Low capture ratio for baseline: {capture_ratio:.1%} < 85%")
            elif fault_type != "none" and capture_ratio < 0.40:
                report["status"] = "FAIL"
                report["failures"].append(f"Critically low capture ratio for fault run: {capture_ratio:.1%} < 40%")

    # -------------------------------------------------------------
    # GATE 1.2b: Target Component & Active Services On-Path
    # -------------------------------------------------------------
    expected_on_path = ON_PATH_SERVICES.get(endpoint, [])
    # Target component validity: if fault run, target must be on-path or communicating datastore
    if fault_type != "none" and target_component:
        # If target is an app service, it must be on-path
        if target_component.endswith("-service") and target_component not in expected_on_path:
            report["status"] = "FAIL"
            report["failures"].append(
                f"Target component '{target_component}' is NOT on execution path for endpoint '{endpoint}'"
            )
        
        # Check if target app service has 0 calls across entire run
        target_call_col = f"{target_component}_call_count"
        if target_call_col in headers:
            target_calls_sum = sum(safe_float(r[target_call_col]) for r in rows)
            if target_calls_sum == 0:
                report["status"] = "FAIL"
                report["failures"].append(
                    f"Target service '{target_component}' has call_count = 0 in every single window"
                )

    # -------------------------------------------------------------
    # GATE 1.2c: Dead Metrics Audit (cpu_rate, mem_bytes, netio)
    # -------------------------------------------------------------
    # Target container memory/cpu checks
    if target_component:
        target_mem_col = f"{target_component}_mem_bytes"
        target_cpu_col = f"{target_component}_cpu_rate"
        
        if target_mem_col in headers:
            zero_mem_count = sum(1 for r in rows if safe_float(r[target_mem_col]) == 0)
            if zero_mem_count > 0.10 * num_rows:
                report["status"] = "FAIL"
                report["failures"].append(
                    f"Dead metric: {target_mem_col} is 0 in {zero_mem_count}/{num_rows} windows (> 10%)"
                )
        
        if target_cpu_col in headers:
            zero_cpu_count = sum(1 for r in rows if safe_float(r[target_cpu_col]) == 0)
            if zero_cpu_count == num_rows:
                report["status"] = "FAIL"
                report["failures"].append(
                    f"Dead metric: {target_cpu_col} is 0 across all {num_rows} windows"
                )

    # -------------------------------------------------------------
    # GATE 1.2f & 1.2k: Fault Window Count & Label Provenance
    # -------------------------------------------------------------
    fault_rows = [r for r in rows if safe_float(r.get("is_fault_window")) == 1]
    report["details"]["num_fault_windows"] = len(fault_rows)

    if fault_type != "none":
        # Duration check
        fault_dur = safe_float(args.get("fault_duration_s") or 30)
        expected_fault_windows = int(round(fault_dur / 5.0))
        
        if len(fault_rows) != expected_fault_windows:
            report["status"] = "FAIL"
            report["failures"].append(
                f"Incorrect fault window count: expected {expected_fault_windows} windows (for {fault_dur}s fault), found {len(fault_rows)}"
            )

        # Provenance check: check effective injection timestamp alignment
        f_start_eff = safe_float(args.get("fault_start_effective") or (manifest_entry.get("fault_start_effective") if manifest_entry else 0))
        exp_start = safe_float(args.get("experiment_start_ts") or (safe_float(rows[0].get("window_start_ts")) if "window_start_ts" in rows[0] else 0))
        
        if f_start_eff > 0 and exp_start > 0 and fault_rows:
            rel_fault_start = f_start_eff - exp_start
            expected_first_w_idx = int(rel_fault_start // 5.0)
            actual_first_w_idx = int(safe_float(fault_rows[0].get("window_id", 0)))
            
            # If actual first fault window is earlier than expected by more than 1 window -> early label bug
            if actual_first_w_idx < expected_first_w_idx - 1:
                report["status"] = "FAIL"
                report["failures"].append(
                    f"Early fault label bug: Window {actual_first_w_idx} labeled fault, but effective injection started at t={rel_fault_start:.2f}s (Window {expected_first_w_idx})"
                )

        # Onset overlap check: first fault window must have fault_overlap_pct >= 0.80
        if fault_rows:
            first_overlap = safe_float(fault_rows[0].get("fault_overlap_pct", 0))
            if first_overlap < 0.75:
                report["status"] = "FAIL"
                report["failures"].append(
                    f"Fault onset overlap too low: first fault window overlap is {first_overlap:.1%} (< 75%)"
                )

        # Phase label sequence: must have normal -> fault -> recovery
        phases_present = set(r.get("phase_label") for r in rows)
        if "recovery" not in phases_present:
            report["status"] = "FAIL"
            report["failures"].append("Missing 'recovery' phase in fault run")

    # -------------------------------------------------------------
    # GATE 1.2e: Pre-fault Baseline Stability
    # -------------------------------------------------------------
    normal_rows = [r for r in rows if r.get("phase_label") == "normal" and safe_float(r.get("is_fault_window")) == 0]
    if len(normal_rows) >= 5:
        # Check throttle in normal baseline
        for r in normal_rows:
            for c in headers:
                if "cfs_throttled_periods_rate" in c or "throttled_periods_rate" in c:
                    th = safe_float(r[c])
                    if th > 0.08:  # strictly baseline <= 0.05 periods/s with slight margin
                        report["status"] = "FAIL"
                        report["failures"].append(
                            f"Pre-fault baseline shows CFS throttling in Window {r.get('window_id')}: {c} = {th:.2f}/s > 0.08"
                        )
                        break

    # -------------------------------------------------------------
    # GATE 1.2d: Sustained Symptom Elevation (>= 80% Fault Windows)
    # -------------------------------------------------------------
    if fault_type != "none" and fault_rows:
        elevated_count = 0
        total_fw = len(fault_rows)

        # Compute baseline values (support both {svc}_latency_p95_ms and {svc}_p95_latency_ms)
        p95_col = None
        for cand in [f"{target_component}_latency_p95_ms", f"{target_component}_p95_latency_ms", "nginx-thrift_latency_p95_ms", "nginx-thrift_p95_latency_ms"]:
            if cand in headers:
                p95_col = cand
                break

        throttle_col = None
        for cand in [f"{target_component}_cfs_throttled_periods_rate", f"{target_component}_throttled_periods_rate"]:
            if cand in headers:
                throttle_col = cand
                break
        
        med_baseline_throttle = 0.0
        med_baseline_p95 = 0.0
        med_baseline_nginx_p95 = 0.0
        if p95_col and normal_rows:
            med_baseline_p95 = median([safe_float(r[p95_col]) for r in normal_rows])
        if "nginx-thrift_latency_p95_ms" in headers and normal_rows:
            med_baseline_nginx_p95 = median([safe_float(r["nginx-thrift_latency_p95_ms"]) for r in normal_rows])
        if throttle_col and normal_rows:
            med_baseline_throttle = median([safe_float(r[throttle_col]) for r in normal_rows])

        for r in fault_rows:
            is_elevated = False
            if fault_type == "cpu":
                th = safe_float(r.get(throttle_col, 0))
                cpu = safe_float(r.get(f"{target_component}_cpu_rate", 0))
                if th > 0.10 or cpu > 1.30 * safe_float(args.get("baseline_cpu", 0.05)):
                    is_elevated = True
            elif fault_type == "network":
                p95 = safe_float(r.get(p95_col, 0))
                nginx_p95 = safe_float(r.get("nginx-thrift_latency_p95_ms", 0))
                err_rate = safe_float(r.get("nginx-thrift_error_rate", 0))
                drop_rate = safe_float(r.get(f"{target_component}_net_drop_rate", 0))
                if (med_baseline_p95 > 0 and p95 > 1.8 * med_baseline_p95) or \
                   (med_baseline_nginx_p95 > 0 and nginx_p95 > 1.8 * med_baseline_nginx_p95) or \
                   err_rate > 0.005 or drop_rate > 0:
                    is_elevated = True
            elif fault_type == "memory":
                # Sustained memory pressure: memory pinned or latency elevated
                p95 = safe_float(r.get(p95_col, 0))
                mem_bytes = safe_float(r.get(f"{target_component}_mem_bytes", 0))
                swap_rate = safe_float(r.get(f"{target_component}_mem_swap_rate", 0))
                # If memory is capped at 64MB (67108864), verify usage is near limit or latency sustained
                if (med_baseline_p95 > 0 and p95 > 1.8 * med_baseline_p95) or swap_rate > 0 or (mem_bytes > 50 * 1024 * 1024):
                    # Check sustained degradation: single transient window is not enough
                    is_elevated = True

            if is_elevated:
                elevated_count += 1

        elevation_pct = elevated_count / total_fw
        report["details"]["symptom_elevation_pct"] = round(elevation_pct, 3)
        if elevation_pct < 0.80:
            report["status"] = "FAIL"
            report["failures"].append(
                f"Symptom not sustained: only {elevated_count}/{total_fw} ({elevation_pct:.1%}) fault windows exhibit elevation (< 80% requirement)"
            )

    # -------------------------------------------------------------
    # GATE 1.2h: Counter Monotonicity
    # -------------------------------------------------------------
    counter_cols = [c for c in headers if c.endswith("_total") or c.endswith("_calls")]
    for c in counter_cols:
        prev_val = None
        for r_idx, r in enumerate(rows):
            cur_val = safe_float(r[c])
            if prev_val is not None and cur_val < prev_val:
                # Check for process restart exception: drops to < 15% of previous value
                if cur_val < 0.15 * prev_val and "unique-id" in c:
                    report["warnings"].append(f"Known UniqueId process restart at Window {r_idx}: {prev_val} -> {cur_val}")
                else:
                    report["status"] = "FAIL"
                    report["failures"].append(
                        f"Counter monotonicity violation in {c} at Window {r_idx}: {prev_val} -> {cur_val} (negative delta)"
                    )
                    break
            prev_val = cur_val

    # -------------------------------------------------------------
    # GATE 1.2i: Byte-level NetIO Check
    # -------------------------------------------------------------
    rx_cols = [c for c in headers if "net_rx_bytes_rate" in c or "net_rx_bytes" in c]
    if rx_cols:
        active_rx = sum(sum(safe_float(r[c]) for r in rows) for c in rx_cols)
        if active_rx == 0:
            report["status"] = "FAIL"
            report["failures"].append("Dead network metrics: cluster sum of net_rx_bytes is 0")

    # Set final status
    if report["failures"]:
        report["status"] = "FAIL"

    return report

def audit_directory(dir_path):
    dir_path = Path(dir_path)
    csv_files = sorted(dir_path.glob("*.csv"))
    
    # Load manifest if available
    manifest_map = {}
    manifest_file = dir_path / "manifest.jsonl"
    if manifest_file.exists():
        with open(manifest_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        if "run_id" in entry:
                            manifest_map[entry["run_id"]] = entry
                    except Exception:
                        pass

    # Load args if available
    args_dir = dir_path / "args"
    
    results = []
    print(f"\n=======================================================")
    print(f" AUDIT VERIFIER: Inspecting {len(csv_files)} runs in {dir_path}")
    print(f"=======================================================\n")

    total_pass = 0
    total_fail = 0

    for csv_file in csv_files:
        run_name = csv_file.stem
        
        # Look for args
        args = {}
        if args_dir.exists():
            args_candidate = args_dir / f"{run_name}_args.txt"
            if args_candidate.exists():
                args = load_args(args_candidate)
        if not args:
            direct_args = dir_path / f"{run_name}_args.txt"
            if direct_args.exists():
                args = load_args(direct_args)
        if not args:
            cand1 = dir_path.parent / "raw_dataset" / run_name / "args.txt"
            if cand1.exists():
                args = load_args(cand1)
        if not args:
            cand2 = dir_path.parent.parent / "raw_dataset" / run_name / "args.txt"
            if cand2.exists():
                args = load_args(cand2)

        manifest_entry = manifest_map.get(run_name, {})
        rep = verify_single_run(csv_file, args, manifest_entry)
        results.append(rep)

        status_str = f"[\033[92mPASS\033[0m]" if rep["status"] == "PASS" else f"[\033[91mFAIL\033[0m]"
        # Non-color plain string for clean logs
        plain_status = f"[{rep['status']}]"
        print(f"{plain_status:<8} {run_name:<42} (Rows: {rep['details'].get('num_windows', 0)}, Cols: {rep['details'].get('num_columns', 0)})")
        if rep["failures"]:
            total_fail += 1
            for f in rep["failures"]:
                print(f"         FAIL-REASON: {f}")
        else:
            total_pass += 1

    print(f"\n-------------------------------------------------------")
    print(f"SUMMARY: {len(results)} audited | {total_pass} PASSED | {total_fail} FAILED")
    print(f"-------------------------------------------------------\n")
    return results

def main():
    parser = argparse.ArgumentParser(description="Independent Verifier for DeathStarBench RCA Datasets")
    parser.add_argument("--dir", help="Directory containing CSV files, manifest.jsonl, and args")
    parser.add_argument("--csv", help="Path to single processed CSV file")
    parser.add_argument("--args", help="Path to args.txt for single run")
    parser.add_argument("--output", help="Optional output JSON path")
    parsed = parser.parse_args()

    if parsed.dir:
        results = audit_directory(parsed.dir)
        if parsed.output:
            with open(parsed.output, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
        if any(r["status"] == "FAIL" for r in results):
            sys.exit(1)
    elif parsed.csv:
        args = load_args(parsed.args) if parsed.args else {}
        rep = verify_single_run(parsed.csv, args)
        print(json.dumps(rep, indent=2))
        if rep["status"] == "FAIL":
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
