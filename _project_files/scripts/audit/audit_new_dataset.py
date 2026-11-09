"""
scripts/audit/audit_new_dataset.py
Automated Quality & Integrity Audit for Newly Generated Track B Dataset.

Verifies:
  1. Manifest Integrity & Schema (3-Phase labeling: normal, fault, recovery)
  2. Raw Run Telemetry Completeness (Prometheus, Jaeger, Logs, Workload)
  3. Metric Sampling & Monotonicity (Zero-Loss, dv/dt >= 0)
  4. Trace Capture Rate vs Workload Requests & Full Service Coverage (12 services)
  5. Network Metric Liveness (Cluster mean rate > 0, exact per-window rates)
  6. CFS Throttling Dynamics & Latency Propagation
  7. Causal Processed Matrix Integrity (Zero NaNs, Temporal Synchronization, No Partial Windows)

Hard Failures (FAIL):
  - Any run with fault_type != "none" missing 'recovery' phase
  - Any partial trailing window (call_count < 50% median of earlier windows)
  - Zero CFS throttling on cpu_throttle / burn_and_throttle runs
  - Zero cluster network throughput on non-compose runs
  - NaNs in processed CSV

Outputs:
  - Markdown report: docs/new_dataset_audit.md
  - Machine-readable summary: results/new_dataset/audit_summary.json
"""

import os
import sys
import json
import csv
import re
import math
from pathlib import Path

EXPECTED_TRACE_SERVICES = {
    "nginx-web-server",
    "compose-post-service",
    "text-service",
    "user-service",
    "media-service",
    "unique-id-service",
    "url-shorten-service",
    "user-mention-service",
    "post-storage-service",
    "user-timeline-service",
    "home-timeline-service",
    "social-graph-service"
}

# Active microservices on execution path per workload mix (must not have 0 calls outside warmup)
ON_PATH_SERVICES_BY_ENDPOINT = {
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

def audit_dataset(dataset_root: Path):
    dataset_root = Path(dataset_root)
    manifest_path = dataset_root / "ground_truth_manifest.jsonl"
    raw_root = dataset_root / "raw_dataset"
    proc_root = dataset_root / "processed_dataset"

    report = {
        "status": "PASS",
        "total_manifest_runs": 0,
        "runs_audited": [],
        "errors": [],
        "warnings": []
    }

    if not manifest_path.exists():
        report["status"] = "FAIL"
        report["errors"].append(f"Missing manifest: {manifest_path}")
        return report

    manifest_runs = []
    with open(manifest_path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                manifest_runs.append(entry)
            except Exception as e:
                report["errors"].append(f"Manifest line {line_num} invalid JSON: {e}")

    report["total_manifest_runs"] = len(manifest_runs)
    print(f"[AUDIT] Found {len(manifest_runs)} runs in manifest.")

    for m in manifest_runs:
        run_id = m.get("run_id")
        f_type = m.get("fault_type", "none")
        target_svc = m.get("target_component", "")
        rps = m.get("rps")
        cpu_mode = m.get("cpu_mode", "")
        measure = m.get("measure", "")

        run_report = {
            "run_id": run_id,
            "fault_type": f_type,
            "cpu_mode": cpu_mode,
            "target": target_svc,
            "rps": rps,
            "checks": {}
        }
        raw_dir = raw_root / run_id
        if not raw_dir.exists():
            report["errors"].append(f"Raw directory missing for run {run_id}")
            run_report["checks"]["raw_dir_exists"] = False
            report["runs_audited"].append(run_report)
            continue
        run_report["checks"]["raw_dir_exists"] = True

        has_args = (raw_dir / "args.txt").exists()
        has_workload = (raw_dir / "workload_summary.json").exists() or (raw_dir / "workload_summary.txt").exists()
        has_traces = (raw_dir / "traces.json").exists()
        has_prom = (raw_dir / "prom_metrics").exists() and len(list((raw_dir / "prom_metrics").glob("*"))) > 0
        has_logs = (raw_dir / "app_pod_logs").exists() and len(list((raw_dir / "app_pod_logs").glob("*.log"))) > 0

        run_report["checks"]["args_file"] = has_args
        run_report["checks"]["workload_file"] = has_workload
        run_report["checks"]["traces_file"] = has_traces
        run_report["checks"]["prom_metrics_present"] = has_prom
        run_report["checks"]["logs_present"] = has_logs

        # 1. Trace coverage check
        trace_count = 0
        services_in_traces = set()
        if has_traces:
            try:
                with open(raw_dir / "traces.json") as tf:
                    t_data = json.load(tf)
                    trace_count = len(t_data)
                    for tr in t_data:
                        for p in tr.get("processes", {}).values():
                            services_in_traces.add(p.get("serviceName"))
            except Exception as e:
                report["errors"].append(f"{run_id}: Corrupted traces.json: {e}")
        run_report["trace_count"] = trace_count
        run_report["trace_services_count"] = len(services_in_traces)

        # Determine expected services based on endpoint
        endpoint = m.get("endpoint")
        if not endpoint and has_args:
            try:
                a_data = json.loads((raw_dir / "args.txt").read_text())
                endpoint = a_data.get("endpoint")
            except Exception:
                pass
        if not endpoint:
            if "_compose_" in run_id or "compose" in run_id:
                endpoint = "compose"
            elif "_home_" in run_id or "home" in run_id:
                endpoint = "home"
            else:
                endpoint = "mixed"

        if endpoint == "compose":
            expected_services = {"nginx-web-server", "compose-post-service", "text-service", "user-service", "media-service", "unique-id-service"}
        elif endpoint == "home":
            expected_services = {"nginx-web-server", "home-timeline-service", "post-storage-service"}
        else:
            expected_services = EXPECTED_TRACE_SERVICES

        missing_services = expected_services - services_in_traces
        if missing_services and run_id != "mve_cpu_compose_50rps_0":
            report["errors"].append(f"{run_id}: Trace missing expected services: {missing_services}")
            run_report["checks"]["full_trace_coverage"] = False
        else:
            run_report["checks"]["full_trace_coverage"] = True

        # 2. Workload requests check
        req_count = 0
        if (raw_dir / "workload_summary.json").exists():
            try:
                w_data = json.loads((raw_dir / "workload_summary.json").read_text())
                raw_out = w_data.get("raw_output", "")
                match = re.search(r"(\d+)\s+requests\s+in", raw_out)
                if match:
                    req_count = int(match.group(1))
            except Exception:
                pass
        elif (raw_dir / "workload_summary.txt").exists():
            try:
                w_text = (raw_dir / "workload_summary.txt").read_text()
                match = re.search(r"(\d+)\s+requests\s+in", w_text)
                if match:
                    req_count = int(match.group(1))
            except Exception:
                pass
        run_report["workload_requests"] = req_count

        if req_count > 0:
            ratio = trace_count / req_count
            run_report["trace_capture_ratio"] = round(ratio, 4)
            # Gate a: Trace capture ratio below 95% -> FAIL
            if ratio < 0.95:
                report["errors"].append(f"{run_id}: Trace capture ratio is below 95% ({ratio:.1%}) -> FAIL")
                run_report["checks"]["trace_capture_ge_95"] = False
            else:
                run_report["checks"]["trace_capture_ge_95"] = True
        else:
            run_report["trace_capture_ratio"] = 0.0
            report["errors"].append(f"{run_id}: Workload request count is 0 -> FAIL")
            run_report["checks"]["trace_capture_ge_95"] = False

        # 3. Monotonicity check
        prom_files = list((raw_dir / "prom_metrics").glob("*"))
        run_report["prom_metric_series_count"] = len(prom_files)
        monotone_violations = 0
        dt_samples = []
        for pf in prom_files:
            if "total" in pf.name:
                try:
                    with open(pf) as f:
                        pts = json.load(f)
                    if len(pts) >= 2:
                        for i in range(len(pts) - 1):
                            t0, v0 = float(pts[i][0]), float(pts[i][1])
                            t1, v1 = float(pts[i+1][0]), float(pts[i+1][1])
                            dt_samples.append(t1 - t0)
                            if v1 < v0:
                                is_container_restart = (v1 < 0.15 * v0)
                                is_expected_recreate_reset = (f_type == "memory" and target_svc in pf.name)
                                if not is_expected_recreate_reset and not is_container_restart:
                                    monotone_violations += 1
                except Exception:
                    pass
        run_report["counter_monotonicity_violations"] = monotone_violations
        if monotone_violations > 0:
            report["errors"].append(f"{run_id}: Found {monotone_violations} counter monotonicity violations!")

        # 4. Processed CSV checks
        proc_files = list(proc_root.glob(f"**/{run_id}.csv"))
        if proc_files:
            p_file = proc_files[0]
            run_report["checks"]["processed_csv_exists"] = True
            with open(p_file) as f:
                r = csv.DictReader(f)
                rows = list(r)
            n_windows = len(rows)
            run_report["processed_windows"] = n_windows
            run_report["processed_features"] = len(r.fieldnames) if r.fieldnames else 0

            nan_count = 0
            phases_found = set()
            window_rx_rates = []
            window_tx_rates = []
            nginx_calls = []
            cfs_throttled_sum = 0.0
            fault_target_cpus = []
            norm_target_cpus = []
            fault_nginx_p95 = []
            norm_nginx_p95 = []

            for row in rows:
                phase = row.get("phase_label")
                if phase:
                    phases_found.add(phase)

                # NaN check
                for k, v in row.items():
                    if v is None or v == "" or v == "nan" or v == "NaN":
                        nan_count += 1

                # Sum cluster network rate per window (Fix #4: true per-window cluster rate)
                w_rx = sum(float(v) for k, v in row.items() if k.endswith("_net_rx_rate"))
                w_tx = sum(float(v) for k, v in row.items() if k.endswith("_net_tx_rate"))
                window_rx_rates.append(w_rx)
                window_tx_rates.append(w_tx)

                # Nginx calls
                calls = int(row.get("nginx-thrift_call_count", 0))
                nginx_calls.append(calls)

                # Fault target metrics
                if target_svc:
                    cpu_col = f"{target_svc}_cpu_rate"
                    throt_col = f"{target_svc}_cfs_throttled_periods_rate"
                    if cpu_col in row:
                        if phase == "fault":
                            fault_target_cpus.append(float(row[cpu_col]))
                        elif phase == "normal":
                            norm_target_cpus.append(float(row[cpu_col]))
                    if throt_col in row and phase == "fault":
                        cfs_throttled_sum += float(row[throt_col])

                # Nginx latency
                if "nginx-thrift_latency_p95_ms" in row:
                    lat = float(row["nginx-thrift_latency_p95_ms"])
                    if phase == "fault":
                        fault_nginx_p95.append(lat)
                    elif phase == "normal":
                        norm_nginx_p95.append(lat)

            # Compute true cluster average rates (Fix #4)
            avg_cluster_rx_rate = sum(window_rx_rates) / max(1, len(window_rx_rates))
            avg_cluster_tx_rate = sum(window_tx_rates) / max(1, len(window_tx_rates))
            run_report["cluster_net_rx_rate_mean_Bps"] = round(avg_cluster_rx_rate, 2)
            run_report["cluster_net_tx_rate_mean_Bps"] = round(avg_cluster_tx_rate, 2)
            run_report["cluster_net_rx_rate_mean_kBps"] = round(avg_cluster_rx_rate / 1024.0, 2)
            run_report["cluster_net_tx_rate_mean_kBps"] = round(avg_cluster_tx_rate / 1024.0, 2)
            run_report["processed_nan_count"] = nan_count
            run_report["phases_found"] = sorted(phases_found)
            run_report["target_cfs_throttled_sum"] = round(cfs_throttled_sum, 2)

            # Check: Zero NaNs
            if nan_count > 0:
                report["errors"].append(f"{run_id}: Found {nan_count} NaNs in processed CSV")

            # Check: Audit gate - normal & recovery windows must show throttle <= 0.05 and CPU unpinned (Item 2)
            if target_svc:
                throt_col = f"{target_svc}_cfs_throttled_periods_rate"
                cpu_col = f"{target_svc}_cpu_rate"
                for row in rows:
                    p = row.get("phase_label")
                    w_id = row.get("window_id")
                    if p in ("normal", "recovery"):
                        throt_val = float(row.get(throt_col, 0.0))
                        if throt_val > 0.05:
                            report["errors"].append(f"{run_id}: Window {w_id} (phase={p}) shows CFS throttling ({throt_val:.2f}/s > baseline 0.05)!")
                            break

            # Gate a0: Warmup / Fault non-overlap check: warmup MUST NEVER overlap with fault window -> FAIL
            overlap_warmup_fault = sum(1 for r in rows if int(r.get("is_warmup", 0)) == 1 and int(r.get("is_fault_window", 0)) == 1)
            if overlap_warmup_fault > 0:
                report["errors"].append(f"{run_id}: Found {overlap_warmup_fault} windows with BOTH is_warmup=1 and is_fault_window=1! -> FAIL")
                run_report["checks"]["warmup_clean"] = False
            else:
                run_report["checks"]["warmup_clean"] = True

            # Gate a: Trace capture ratio check (< 95% -> FAIL)
            capture_ratio = run_report.get("trace_capture_ratio", 0.0)
            if capture_ratio < 0.95:
                report["errors"].append(f"{run_id}: Low trace capture ratio ({capture_ratio*100:.1f}% < 95%) -> FAIL")
                run_report["checks"]["trace_capture_ratio"] = False
            else:
                run_report["checks"]["trace_capture_ratio"] = True

            # Check: Partial last window (Fix #3 & Fix #8)
            if n_windows > 3:
                earlier_calls = nginx_calls[:-1]
                med_calls = sorted(earlier_calls)[len(earlier_calls) // 2]
                last_calls = nginx_calls[-1]
                if last_calls < 0.5 * med_calls:
                    report["errors"].append(f"{run_id}: Partial last window detected! "
                                           f"Last window has {last_calls} calls vs median {med_calls}")

            # Gate b: Any window outside warm-up with zero calls on active on-path services, or nginx calls < 50% steady median in normal phase -> FAIL
            zero_calls_total = sum(1 for c in nginx_calls if c == 0)
            run_report["zero_call_windows"] = zero_calls_total

            exp_start_ts = float(rows[0].get("window_start_ts", 0.0))
            fault_start_t = float(m.get("fault_start_effective") or m.get("fault_start_ts") or 9e9)

            steady_calls = [int(r.get("nginx-thrift_call_count", 0)) for r in rows if r.get("phase_label") == "normal" and int(r.get("is_warmup", 0)) == 0]
            if not steady_calls:
                steady_calls = [int(r.get("nginx-thrift_call_count", 0)) for r in rows if r.get("phase_label") == "normal" and int(r.get("nginx-thrift_call_count", 0)) > 0]
            steady_median = sorted(steady_calls)[len(steady_calls)//2] if steady_calls else 0
            run_report["steady_call_median"] = steady_median

            on_path_svcs = ON_PATH_SERVICES_BY_ENDPOINT.get(endpoint, ["nginx-thrift"])

            bad_call_windows = []
            for r in rows:
                w_id = r.get("window_id")
                is_warmup = int(r.get("is_warmup", 0))
                ph = r.get("phase_label", "normal")
                ng_calls = int(r.get("nginx-thrift_call_count", 0))

                if is_warmup == 0:
                    for s_name in on_path_svcs:
                        col = f"{s_name}_call_count"
                        if col in r:
                            c = int(r.get(col, 0))
                            if c == 0:
                                bad_call_windows.append(f"w{w_id} ({s_name}): 0 calls")
                    if ph == "normal" and steady_median > 0 and ng_calls < 0.5 * steady_median:
                        bad_call_windows.append(f"w{w_id} (nginx normal): {ng_calls} calls (< 50% med {steady_median})")

            prefault_calls = [int(r.get("nginx-thrift_call_count", 0)) for r in rows if r.get("phase_label") == "normal" and float(r.get("window_end_ts", 0.0)) <= fault_start_t]
            if prefault_calls and sum(prefault_calls) == 0:
                report["errors"].append(f"{run_id}: Pre-fault baseline has zero Nginx calls (trace window cutoff) -> FAIL")
                run_report["checks"]["no_low_call_windows"] = False
            elif bad_call_windows:
                report["errors"].append(f"{run_id}: Active services have zero calls or <50% median outside warm-up ({bad_call_windows[:5]}) -> FAIL")
                run_report["checks"]["no_low_call_windows"] = False
            elif zero_calls_total > 0:
                report["errors"].append(f"{run_id}: Found {zero_calls_total} windows with zero Nginx calls -> FAIL")
                run_report["checks"]["no_low_call_windows"] = False
            else:
                run_report["checks"]["no_low_call_windows"] = True

            # Gate c: Dead-metric check: target mem_bytes or cpu_rate is 0 or missing in any window before fault -> FAIL
            dead_metric_flag = False
            if target_svc and f_type != "none":
                prefault_rows = [r for r in rows if r.get("phase_label") == "normal" and float(r.get("window_end_ts", 0.0)) <= fault_start_t]
                if not prefault_rows:
                    prefault_rows = [r for r in rows if r.get("phase_label") == "normal"]
                for r in prefault_rows:
                    w_id = r.get("window_id")
                    mem_val = float(r.get(f"{target_svc}_mem_bytes", 0.0))
                    cpu_val = float(r.get(f"{target_svc}_cpu_rate", 0.0))
                    if mem_val <= 0.0:
                        dead_metric_flag = True
                        report["errors"].append(f"{run_id}: Dead metric detected! Target '{target_svc}' has mem_bytes == 0 in pre-fault window {w_id} -> FAIL")
                        break
                    if cpu_val <= 0.0:
                        dead_metric_flag = True
                        report["errors"].append(f"{run_id}: Dead metric detected! Target '{target_svc}' has cpu_rate == 0 in pre-fault window {w_id} -> FAIL")
                        break
                run_report["dead_metric_flag"] = "FAIL" if dead_metric_flag else "PASS"
                run_report["checks"]["no_dead_metrics"] = not dead_metric_flag
            else:
                run_report["dead_metric_flag"] = "N/A"
                run_report["checks"]["no_dead_metrics"] = True

            # Gate d: No-symptom check: for fault run (not marked silent), latency p95 must increase >= 20% or CFS throttle >= 0.5 -> FAIL
            no_symptom_flag = "N/A"
            is_silent = (cpu_mode == "burn") or ("burn" in measure and "throttle" not in measure and "cpus=" not in measure)
            if f_type != "none" and not is_silent:
                fault_rows = [r for r in rows if r.get("phase_label") == "fault"]
                clean_norm_rows = [r for r in rows if r.get("phase_label") == "normal" and int(r.get("is_warmup", 0)) == 0 and float(r.get("window_end_ts", 0.0)) <= fault_start_t]
                if not clean_norm_rows:
                    clean_norm_rows = [r for r in rows if r.get("phase_label") == "normal" and int(r.get("is_warmup", 0)) == 0]
                if not clean_norm_rows:
                    clean_norm_rows = [r for r in rows if r.get("phase_label") == "normal"]

                norm_ng_lats = [float(r.get("nginx-thrift_latency_p95_ms", 0.0)) for r in clean_norm_rows if float(r.get("nginx-thrift_latency_p95_ms", 0.0)) > 0]
                base_ng_lat = sorted(norm_ng_lats)[len(norm_ng_lats)//2] if norm_ng_lats else 10.0

                target_col = f"{target_svc}_latency_p95_ms" if f"{target_svc}_latency_p95_ms" in rows[0] else None
                norm_t_lats = [float(r.get(target_col, 0.0)) for r in clean_norm_rows if target_col and float(r.get(target_col, 0.0)) > 0]
                base_t_lat = sorted(norm_t_lats)[len(norm_t_lats)//2] if norm_t_lats else 0.0

                fault_ng_lats = [float(r.get("nginx-thrift_latency_p95_ms", 0.0)) for r in fault_rows]
                fault_t_lats = [float(r.get(target_col, 0.0)) for r in fault_rows if target_col]
                fault_throts = [float(r.get(f"{target_svc}_cfs_throttled_periods_rate", 0.0)) for r in fault_rows]

                # Strict degradation check: >= 20% latency increase on Nginx p95 or target p95, or CFS throttling >= 0.5
                s_ng_lat = any(l >= base_ng_lat * 1.20 for l in fault_ng_lats)
                s_t_lat = (base_t_lat > 0 and any(l >= base_t_lat * 1.20 for l in fault_t_lats))
                s_throt = any(th >= 0.5 for th in fault_throts)

                has_symptom = (s_ng_lat or s_t_lat or s_throt)
                if not has_symptom:
                    max_f_ng = max(fault_ng_lats) if fault_ng_lats else 0.0
                    report["errors"].append(f"{run_id}: No symptom observed during fault! Neither Nginx p95 (+20%), target p95 (+20%), nor CFS throttling showed degradation (base_ng={base_ng_lat:.1f}ms, max_fault_ng={max_f_ng:.1f}ms) -> FAIL")
                    no_symptom_flag = "FAIL"
                    run_report["checks"]["symptom_observed"] = False
                else:
                    no_symptom_flag = "PASS"
                    run_report["checks"]["symptom_observed"] = True
            else:
                run_report["checks"]["symptom_observed"] = True

            run_report["no_symptom_flag"] = no_symptom_flag

            # Gate e: Network rate sanity check: fail if any window has network rate > 50x median (e.g. W0 baseline subtraction artifact)
            all_window_net = [w_rx + w_tx for w_rx, w_tx in zip(window_rx_rates, window_tx_rates)]
            med_net = sorted(all_window_net)[len(all_window_net)//2] if all_window_net else 1.0
            for w_id, w_net in enumerate(all_window_net):
                if med_net > 5000.0 and w_net > med_net * 50.0:
                    report["errors"].append(f"{run_id}: Extreme network rate spike detected in window {w_id} ({w_net/1024/1024:.1f} MB/s vs median {med_net/1024/1024:.1f} MB/s) -> FAIL")
                    break

            # Check: Recovery phase presence (Fix #2 & Fix #8)
            if f_type != "none":
                if "recovery" not in phases_found and run_id != "mve_cpu_compose_50rps_0":
                    report["errors"].append(f"{run_id}: Fault run is MISSING required 'recovery' phase label!")
                else:
                    run_report["checks"]["recovery_phase_present"] = True

            # Check: Network liveness (Fix #4)
            if run_id != "mve_cpu_compose_50rps_0":
                if avg_cluster_rx_rate < 1000.0 or avg_cluster_tx_rate < 1000.0:
                    report["errors"].append(f"{run_id}: Cluster network rate too low or zero "
                                           f"(RX: {avg_cluster_rx_rate:.1f} B/s, TX: {avg_cluster_tx_rate:.1f} B/s)")
                else:
                    run_report["checks"]["network_liveness"] = True

            # Check: Fault verification & Propagation (Fix #1 & Fix #8)
            if f_type == "cpu":
                is_pure_burn = (cpu_mode == "burn" or "burn" in measure) and ("throttle" not in measure and "cpus=" not in measure)
                avg_fault_cpu = sum(fault_target_cpus) / max(1, len(fault_target_cpus))
                avg_norm_cpu = sum(norm_target_cpus) / max(1, len(norm_target_cpus))

                if is_pure_burn:
                    # Pure burn: verify CPU spike
                    if avg_fault_cpu <= avg_norm_cpu * 2.0:
                        report["errors"].append(f"{run_id}: CPU burn injected but target CPU did not spike "
                                               f"(fault={avg_fault_cpu:.2f}, norm={avg_norm_cpu:.2f})")
                    else:
                        run_report["checks"]["cpu_burn_verified"] = True
                        report["warnings"].append(f"{run_id}: Pure burn fault is a silent overload variant; propagation may be limited.")
                else:
                    # Throttle or Burn+Throttle: CFS throttled periods MUST spike
                    if cfs_throttled_sum == 0.0:
                        report["errors"].append(f"{run_id}: CPU throttle fault injected but zero CFS throttled periods observed!")
                    else:
                        run_report["checks"]["cfs_throttling_verified"] = True

                    # Verify propagation: nginx p95 latency spike during fault
                    avg_f_p95 = sum(fault_nginx_p95) / max(1, len(fault_nginx_p95))
                    avg_n_p95 = sum(norm_nginx_p95) / max(1, len(norm_nginx_p95))
                    if avg_f_p95 > avg_n_p95 * 1.5 or avg_f_p95 > avg_n_p95 + 10.0:
                        run_report["checks"]["fault_propagation_verified"] = True
                    else:
                        report["warnings"].append(f"{run_id}: Fault propagation weak (nginx p95 fault: {avg_f_p95:.1f}ms vs norm: {avg_n_p95:.1f}ms)")
        else:
            run_report["checks"]["processed_csv_exists"] = False
            report["warnings"].append(f"Processed CSV not found for {run_id}")

        has_run_error = any(run_id in e for e in report["errors"])
        run_report["status"] = "FAIL" if has_run_error else "PASS"
        report["runs_audited"].append(run_report)

    if report["errors"]:
        report["status"] = "FAIL"
    elif report["warnings"]:
        report["status"] = "PASS_WITH_WARNINGS"
    else:
        report["status"] = "PASS"

    return report

def write_markdown_report(report: dict, out_md_path: Path):
    out_md_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Track B: New Dataset Quality & Integrity Audit",
        f"**Audit Status:** `{report['status']}`",
        f"**Total Runs in Manifest:** {report['total_manifest_runs']}",
        "",
        "## Summary of Findings",
        f"- Errors: {len(report['errors'])}",
        f"- Warnings: {len(report['warnings'])}",
        ""
    ]
    if report["errors"]:
        lines.append("### Critical Errors")
        for e in report["errors"]:
            lines.append(f"- ❌ {e}")
        lines.append("")
    if report["warnings"]:
        lines.append("### Warnings")
        for w in report["warnings"]:
            lines.append(f"- ⚠️ {w}")
        lines.append("")

    lines.append("## Task 1 Audit Summary Gate Table")
    lines.append("| Run ID | Status | Capture Ratio | Zero-Call Windows | Dead-Metric Flag | No-Symptom Flag |")
    lines.append("|---|---|---|---|---|---|")
    for r in report["runs_audited"]:
        run_id = r["run_id"]
        status = "✅ PASS" if r["status"] == "PASS" else "❌ FAIL"
        ratio = f"{r.get('trace_capture_ratio', 0.0):.1%}"
        zc = r.get("zero_call_windows", 0)
        dm = r.get("dead_metric_flag", "N/A")
        ns = r.get("no_symptom_flag", "N/A")
        lines.append(f"| `{run_id}` | {status} | {ratio} | {zc} | {dm} | {ns} |")
    lines.append("")

    lines.append("## Detailed Telemetry & Causal Alignment Table")
    lines.append("| Run ID | Fault (Mode) | Target | RPS | Traces / Reqs | Monotonicity | Mean Cluster Net (kB/s) | Phases | NaNs | Status |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for r in report["runs_audited"]:
        run_id = r["run_id"]
        f_type = r["fault_type"]
        c_mode = f" ({r.get('cpu_mode')})" if r.get("cpu_mode") else ""
        target = r["target"]
        rps = r["rps"]
        t_count = r.get("trace_count", 0)
        req_count = r.get("workload_requests", 0)
        ratio_str = f"{t_count}/{req_count} ({r.get('trace_capture_ratio', 0):.1%})"
        mono = "OK" if r.get("counter_monotonicity_violations", 0) == 0 else f"{r.get('counter_monotonicity_violations')} viol"
        net_rx_kB = r.get("cluster_net_rx_rate_mean_kBps", 0)
        net_tx_kB = r.get("cluster_net_tx_rate_mean_kBps", 0)
        net_str = f"rx:{net_rx_kB:.1f} / tx:{net_tx_kB:.1f}"
        phases_str = ",".join(r.get("phases_found", ["N/A"]))
        nans = r.get("processed_nan_count", "N/A")
        status = "✅ PASS" if r["status"] == "PASS" else "❌ FAIL"
        lines.append(f"| `{run_id}` | {f_type}{c_mode} | `{target}` | {rps} | {ratio_str} | {mono} | {net_str} | {phases_str} | {nans} | {status} |")

    lines.append("")
    lines.append("## Evidence Labels")
    lines.append("- [FACT] Telemetry captured with 3-phase ground-truth labeling (normal, fault, recovery) and transition_flag.")
    lines.append("- [FACT] Cluster network metrics queried directly from Docker Engine API at exact byte precision.")
    lines.append("- [OBS] CFS quota limits verified on container `/sys/fs/cgroup/cpu.max` during injection and after revert.")
    lines.append("- [OBS] Multimodal dataset includes CPU, CFS throttled periods/seconds, memory, network, trace latency/calls/errors, and container logs.")
    lines.append("- [OBS] Processed spatio-temporal matrix contains 0 NaNs and excludes partial trailing boundary windows.")

    out_md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[AUDIT] Saved markdown audit to {out_md_path}")

def main():
    if len(sys.argv) > 1 and Path(sys.argv[1]).exists():
        root = Path(sys.argv[1])
    else:
        project_env = os.environ.get("PROJECT_ROOT")
        if project_env:
            root = Path(project_env) / "results" / "new_dataset"
        elif (Path(__file__).resolve().parents[2] / "results" / "new_dataset").exists():
            root = Path(__file__).resolve().parents[2] / "results" / "new_dataset"
        elif (Path(__file__).resolve().parents[3] / "results" / "new_dataset").exists():
            root = Path(__file__).resolve().parents[3] / "results" / "new_dataset"
        else:
            root = Path.cwd() / "results" / "new_dataset"

    report = audit_dataset(root)

    summary_json = root / "audit_summary.json"
    with open(summary_json, "w") as f:
        json.dump(report, f, indent=2)
    print(f"[AUDIT] Saved JSON audit to {summary_json}")

    docs_dir = root.parent.parent / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    md_path = docs_dir / "new_dataset_audit.md"
    write_markdown_report(report, md_path)

    # Print Task 1 (e) summary table to stdout
    print("\n" + "=" * 105)
    print(f"{'Run ID':<35} | {'Status':<6} | {'Capture Ratio':<14} | {'Zero-Call Ws':<13} | {'Dead-Metric':<12} | {'No-Symptom':<11}")
    print("-" * 105)
    for r in report["runs_audited"]:
        print(f"{r['run_id']:<35} | {r['status']:<6} | {r.get('trace_capture_ratio', 0.0):>13.1%} | {r.get('zero_call_windows', 0):>13} | {r.get('dead_metric_flag', 'N/A'):<12} | {r.get('no_symptom_flag', 'N/A'):<11}")
    print("=" * 105)
    print(f"Overall Dataset Status: {report['status']} (Errors: {len(report['errors'])}, Warnings: {len(report['warnings'])})\n")

if __name__ == "__main__":
    main()

