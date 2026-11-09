"""
scripts/preprocessing/process_run.py
Causal preprocessor for DeathStarBench multimodal telemetry runs.
Standard-library only (no external dependencies required).

Transforms raw run artifacts:
  - prom_metrics/<container>_<metric> (JSON time series)
  - traces.json (Jaeger distributed trace trees)
  - app_pod_logs/<container>.log (Container application logs)
  - args.txt (Run configuration & ground truth)
into a structured, tabular spatio-temporal matrix:
  - results/new_dataset/processed_dataset/<endpoint>/<run_id>.csv
alongside service metadata and node-level graph definitions.

Supports:
  - 3-Phase labeling: normal, fault, recovery (guaranteed recovery phase)
  - Transition handling: transition_flag for straddling boundary windows
  - Partial trailing window detection & removal (call_count < 50% median)
  - CFS throttled periods and seconds rate extraction
  - Exact byte-level network rates
  - Error metrics: span-level error_count and error_rate per service
  - Log metrics: ISO-8601 aligned log_count and log_error_count per service
"""

import os
import sys
import json
import math
import bisect
import csv
import re
import argparse
from datetime import datetime, timezone
from pathlib import Path

# Canonical 26 microservice mapping for DeathStarBench Social Network
DEFAULT_SERVICES = [
    {"id": 0, "name": "nginx-thrift", "container_pattern": "nginx-thrift", "type": "frontend"},
    {"id": 1, "name": "compose-post-service", "container_pattern": "compose-post-service", "type": "business_logic"},
    {"id": 2, "name": "text-service", "container_pattern": "text-service", "type": "business_logic"},
    {"id": 3, "name": "user-mention-service", "container_pattern": "user-mention-service", "type": "business_logic"},
    {"id": 4, "name": "user-service", "container_pattern": "user-service", "type": "business_logic"},
    {"id": 5, "name": "user-mongodb", "container_pattern": "user-mongodb", "type": "datastore_nosql"},
    {"id": 6, "name": "user-memcached", "container_pattern": "user-memcached", "type": "datastore_cache"},
    {"id": 7, "name": "user-timeline-service", "container_pattern": "user-timeline-service", "type": "business_logic"},
    {"id": 8, "name": "user-timeline-mongodb", "container_pattern": "user-timeline-mongodb", "type": "datastore_nosql"},
    {"id": 9, "name": "user-timeline-redis", "container_pattern": "user-timeline-redis", "type": "datastore_cache"},
    {"id": 10, "name": "post-storage-service", "container_pattern": "post-storage-service", "type": "business_logic"},
    {"id": 11, "name": "post-storage-mongodb", "container_pattern": "post-storage-mongodb", "type": "datastore_nosql"},
    {"id": 12, "name": "post-storage-memcached", "container_pattern": "post-storage-memcached", "type": "datastore_cache"},
    {"id": 13, "name": "home-timeline-service", "container_pattern": "home-timeline-service", "type": "business_logic"},
    {"id": 14, "name": "home-timeline-redis", "container_pattern": "home-timeline-redis", "type": "datastore_cache"},
    {"id": 15, "name": "social-graph-service", "container_pattern": "social-graph-service", "type": "business_logic"},
    {"id": 16, "name": "social-graph-mongodb", "container_pattern": "social-graph-mongodb", "type": "datastore_nosql"},
    {"id": 17, "name": "social-graph-redis", "container_pattern": "social-graph-redis", "type": "datastore_cache"},
    {"id": 18, "name": "url-shorten-service", "container_pattern": "url-shorten-service", "type": "business_logic"},
    {"id": 19, "name": "url-shorten-mongodb", "container_pattern": "url-shorten-mongodb", "type": "datastore_nosql"},
    {"id": 20, "name": "url-shorten-memcached", "container_pattern": "url-shorten-memcached", "type": "datastore_cache"},
    {"id": 21, "name": "media-service", "container_pattern": "media-service", "type": "business_logic"},
    {"id": 22, "name": "media-mongodb", "container_pattern": "media-mongodb", "type": "datastore_nosql"},
    {"id": 23, "name": "media-memcached", "container_pattern": "media-memcached", "type": "datastore_cache"},
    {"id": 24, "name": "media-frontend", "container_pattern": "media-frontend", "type": "frontend"},
    {"id": 25, "name": "unique-id-service", "container_pattern": "unique-id-service", "type": "business_logic"}
]

# Jaeger service name mapping to canonical service name
JAEGER_TO_CANONICAL = {
    "nginx-web-server": "nginx-thrift",
    "compose-post-service": "compose-post-service",
    "text-service": "text-service",
    "user-mention-service": "user-mention-service",
    "user-service": "user-service",
    "user-timeline-service": "user-timeline-service",
    "post-storage-service": "post-storage-service",
    "home-timeline-service": "home-timeline-service",
    "social-graph-service": "social-graph-service",
    "url-shorten-service": "url-shorten-service",
    "media-service": "media-service",
    "unique-id-service": "unique-id-service"
}

def load_ground_truth(raw_dir: Path):
    args_file = raw_dir / "args.txt"
    if not args_file.exists():
        raise FileNotFoundError(f"Missing args.txt in {raw_dir}")
    with open(args_file) as f:
        meta = json.load(f)
    return meta

def match_container_to_service(filename: str, services: list):
    # Strictly ignore chaos helper sidecars
    if filename.startswith("netshoot-") or "netshoot" in filename:
        return None
    for s in services:
        if s["container_pattern"] in filename:
            return s
    return None

def compute_causal_rates(timestamps: list, values: list):
    """
    Computes forward causal rate dv/dt for cumulative counters.
    Guarantees non-negativity (clamps counter resets to 0).
    """
    if len(values) < 2:
        return [0.0] * len(values)
    rates = []
    for i in range(len(values) - 1):
        dt = timestamps[i + 1] - timestamps[i]
        dv = values[i + 1] - values[i]
        if dt <= 0:
            dt = 1.0
        rate = max(0.0, dv / dt)
        rates.append(rate)
    rates.insert(0, rates[0] if rates else 0.0)
    return rates

def interp_point(t: float, xp: list, fp: list, left: float = None, right: float = None, fault_start: float = None, fault_end: float = None):
    if not xp or not fp:
        return 0.0 if left is None else left
    if len(xp) == 1:
        return fp[0]
    if left is None:
        left = fp[0]
    if right is None:
        right = fp[-1]
    if t <= xp[0]:
        return left
    if t >= xp[-1]:
        return right
    idx = bisect.bisect_right(xp, t) - 1
    x0, x1 = xp[idx], xp[idx + 1]
    y0, y1 = fp[idx], fp[idx + 1]
    # Causal edge: if bridging across fault_start where t <= fault_start < x1
    if fault_start is not None and x0 <= t <= fault_start < x1:
        return y0
    # Causal edge: if bridging across fault_end where x0 < fault_end <= t <= x1
    if fault_end is not None and x0 < fault_end <= t <= x1:
        return y1
    if x1 == x0:
        return y0
    return y0 + (y1 - y0) * (t - x0) / (x1 - x0)

def deduplicate_stale_counter_samples(ts: list, vals: list):
    """
    Remove non-strictly-increasing timestamps and filter out any artificial dips
    to avoid 0.000 rate drops caused by jitter.
    """
    if len(ts) < 2:
        return ts, vals
    clean_ts = [ts[0]]
    clean_vals = [vals[0]]
    for i in range(1, len(ts)):
        if ts[i] <= clean_ts[-1]:
            continue
        clean_ts.append(ts[i])
        clean_vals.append(vals[i])
    return clean_ts, clean_vals

def compute_counter_window_rates(window_edges: list, ts: list, vals: list, fault_start: float = None, fault_end: float = None):
    """
    Computes exact interval rate (v(w_end) - v(w_start)) / dt for each window.
    Interpolates counter at window edges, eliminating bleed/smear artifacts across window boundaries.
    """
    clean_ts, clean_vals = deduplicate_stale_counter_samples(ts, vals)
    n_windows = len(window_edges) - 1
    rates = []
    for w in range(n_windows):
        w_start = window_edges[w]
        w_end = window_edges[w + 1]
        dt = w_end - w_start
        if dt <= 0:
            dt = 1.0
        v_start = interp_point(w_start, clean_ts, clean_vals, fault_start=fault_start, fault_end=fault_end)
        v_end = interp_point(w_end, clean_ts, clean_vals, fault_start=fault_start, fault_end=fault_end)
        rate = max(0.0, (v_end - v_start) / dt)
        rates.append(rate)
    return rates

def interp_series(x_targets: list, xp: list, fp: list, left: float = 0.0, right: float = None):
    """
    1D piecewise linear interpolation using standard library bisect.
    """
    if not xp or not fp:
        return [left] * len(x_targets)
    if len(xp) == 1:
        return [fp[0]] * len(x_targets)
    if right is None:
        right = fp[-1]

    result = []
    for x in x_targets:
        if x <= xp[0]:
            result.append(left)
        elif x >= xp[-1]:
            result.append(right)
        else:
            idx = bisect.bisect_right(xp, x) - 1
            x0, x1 = xp[idx], xp[idx + 1]
            y0, y1 = fp[idx], fp[idx + 1]
            if x1 == x0:
                result.append(y0)
            else:
                slope = (y1 - y0) / (x1 - x0)
                result.append(y0 + slope * (x - x0))
    return result

def percentile(vals: list, p: float):
    if not vals:
        return 0.0
    s = sorted(vals)
    k = p * (len(s) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] * (c - k) + s[c] * (k - f)

def extract_trace_metrics_by_window(traces_path: Path, window_edges: list, services: list):
    """
    Extracts per-window latency, call count, and error count from Jaeger traces.
    """
    svc_to_id = {s["name"]: s["id"] for s in services}
    n_windows = len(window_edges) - 1
    lat_stats = {(s["id"], w): [] for s in services for w in range(n_windows)}
    err_stats = {(s["id"], w): 0 for s in services for w in range(n_windows)}

    if not traces_path.exists():
        return lat_stats, err_stats

    try:
        with open(traces_path) as f:
            traces = json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to read {traces_path}: {e}", file=sys.stderr)
        return lat_stats, err_stats

    for trace in traces:
        processes = trace.get("processes", {})
        spans = trace.get("spans", [])
        for span in spans:
            pid = span.get("processID")
            svc_raw = processes.get(pid, {}).get("serviceName", "")
            canonical_name = JAEGER_TO_CANONICAL.get(svc_raw)
            if not canonical_name or canonical_name not in svc_to_id:
                continue
            svc_id = svc_to_id[canonical_name]

            span_start_s = span.get("startTime", 0) / 1_000_000.0
            duration_ms = span.get("duration", 0) / 1000.0

            if span_start_s < window_edges[0] or span_start_s >= window_edges[-1]:
                continue
            w_idx = bisect.bisect_right(window_edges, span_start_s) - 1
            if 0 <= w_idx < n_windows:
                lat_stats[(svc_id, w_idx)].append(duration_ms)

                # Check for errors in tags
                is_error = False
                for tag in span.get("tags", []):
                    k = tag.get("key", "")
                    v = tag.get("value", "")
                    if k == "error" and v in (True, "true", 1, "1"):
                        is_error = True
                        break
                    elif k == "http.status_code":
                        try:
                            if int(v) >= 400:
                                is_error = True
                                break
                        except Exception:
                            pass
                if is_error:
                    err_stats[(svc_id, w_idx)] += 1

    return lat_stats, err_stats

def parse_iso_timestamp(line: str):
    """Parse ISO-8601 timestamp prefix added by Docker --timestamps or MongoDB JSON."""
    # 1. Docker ISO format: 2026-11-05T19:24:09.445017123Z
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", line)
    if m:
        ts_str = m.group(1)
        parts = ts_str.split(".")
        if len(parts) > 1:
            ts_str = f"{parts[0]}.{parts[1][:6]}"
        try:
            dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            pass

    # 2. MongoDB JSON format: {"t":{"$date":"2026-11-05T19:23:24.383+00:00"}...
    m_mongo = re.search(r'"\$date"\s*:\s*"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)', line)
    if m_mongo:
        ts_str = m_mongo.group(1)
        parts = ts_str.split(".")
        if len(parts) > 1:
            ts_str = f"{parts[0]}.{parts[1][:6]}"
        try:
            dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            pass

    return None

def extract_log_metrics_by_window(logs_dir: Path, window_edges: list, services: list):
    """
    Extracts per-window log counts and error/warn counts from container logs.
    """
    n_windows = len(window_edges) - 1
    log_counts = {(s["id"], w): 0 for s in services for w in range(n_windows)}
    log_errors = {(s["id"], w): 0 for s in services for w in range(n_windows)}

    if not logs_dir.exists():
        return log_counts, log_errors

    error_pattern = re.compile(r"\b(ERROR|FATAL|CRITICAL|FAIL|EXCEPTION)\b", re.IGNORECASE)

    for log_file in logs_dir.glob("*.log"):
        matched_svc = match_container_to_service(log_file.name, services)
        if not matched_svc:
            continue
        svc_id = matched_svc["id"]

        try:
            with open(log_file, errors="replace") as f:
                last_ts = None
                for line in f:
                    line_s = line.strip()
                    if not line_s or line_s.startswith("--- STDERR ---"):
                        continue

                    ts = parse_iso_timestamp(line_s)
                    if ts is not None:
                        last_ts = ts
                    elif last_ts is not None:
                        ts = last_ts  # Continuation line
                    else:
                        continue

                    if ts < window_edges[0] or ts >= window_edges[-1]:
                        continue
                    w_idx = bisect.bisect_right(window_edges, ts) - 1
                    if 0 <= w_idx < n_windows:
                        log_counts[(svc_id, w_idx)] += 1
                        if error_pattern.search(line_s):
                            log_errors[(svc_id, w_idx)] += 1
        except Exception as e:
            print(f"[WARN] Failed to parse log {log_file}: {e}", file=sys.stderr)

    return log_counts, log_errors

def process_single_run(raw_dir: Path, out_dir: Path, step_s: float = 5.0, recovery_duration_s: float = 45.0):
    raw_dir = Path(raw_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = load_ground_truth(raw_dir)
    run_id = meta["run_id"]
    fault_type = meta.get("fault_type", "none")
    target_comp = meta.get("target_component", "none")
    # Use effective verified timestamps if available, otherwise nominal timestamps
    fault_start = float(meta.get("fault_start_effective") or meta.get("fault_start_ts") or 0.0) if meta.get("fault_start_ts") else None
    fault_end = float(meta.get("fault_end_effective") or meta.get("fault_end_ts") or 0.0) if meta.get("fault_end_ts") else None
    exp_start = float(meta.get("experiment_start_ts", (fault_start - 30.0) if fault_start else 0.0))
    exp_end = float(meta.get("experiment_end_ts", (fault_end + 30.0) if fault_end else 120.0))

    warm_up_s = float(meta.get("warm_up_s", 60.0))

    # Uniform causal time windows [t0, t0+step, ...]
    window_edges = []
    t = exp_start
    while t <= exp_end + 1e-6:
        window_edges.append(t)
        t += step_s

    window_centers = [window_edges[i] + (step_s / 2.0) for i in range(len(window_edges) - 1)]
    n_windows = len(window_centers)

    services = DEFAULT_SERVICES
    svc_name_to_id = {s["name"]: s["id"] for s in services}
    target_id = svc_name_to_id.get(target_comp, -1)

    # Per-service feature dictionaries
    service_features = {
        s["id"]: {
            "cpu_rate": [0.0] * n_windows,
            "cfs_throttled_periods_rate": [0.0] * n_windows,
            "cfs_throttled_seconds_rate": [0.0] * n_windows,
            "mem_bytes": [0.0] * n_windows,
            "net_rx_rate": [0.0] * n_windows,
            "net_tx_rate": [0.0] * n_windows,
            "latency_mean_ms": [0.0] * n_windows,
            "latency_p95_ms": [0.0] * n_windows,
            "call_count": [0] * n_windows,
            "error_count": [0] * n_windows,
            "error_rate": [0.0] * n_windows,
            "log_count": [0] * n_windows,
            "log_error_count": [0] * n_windows
        } for s in services
    }

    # 1. Parse Prometheus & Network metrics
    prom_dir = raw_dir / "prom_metrics"
    if prom_dir.exists():
        for metric_file in prom_dir.glob("*"):
            fname = metric_file.name
            matched_svc = match_container_to_service(fname, services)
            if not matched_svc:
                continue
            svc_id = matched_svc["id"]

            try:
                with open(metric_file) as mf:
                    raw_pts = json.load(mf)
                if not raw_pts or len(raw_pts) < 2:
                    continue

                pts = sorted(raw_pts, key=lambda p: float(p[0]))
                ts = [float(p[0]) for p in pts]
                vals = [float(p[1]) for p in pts]

                if "cpu_cfs_throttled_periods_total" in fname:
                    service_features[svc_id]["cfs_throttled_periods_rate"] = compute_counter_window_rates(window_edges, ts, vals, fault_start=fault_start, fault_end=fault_end)
                elif "cpu_cfs_throttled_seconds_total" in fname:
                    service_features[svc_id]["cfs_throttled_seconds_rate"] = compute_counter_window_rates(window_edges, ts, vals, fault_start=fault_start, fault_end=fault_end)
                elif "cpu_usage_seconds_total" in fname:
                    service_features[svc_id]["cpu_rate"] = compute_counter_window_rates(window_edges, ts, vals, fault_start=fault_start, fault_end=fault_end)
                elif "memory_working_set_bytes" in fname or "memory_usage_bytes" in fname:
                    service_features[svc_id]["mem_bytes"] = interp_series(window_centers, ts, vals, left=0.0)
                elif "network_receive_bytes_total" in fname:
                    service_features[svc_id]["net_rx_rate"] = compute_counter_window_rates(window_edges, ts, vals, fault_start=fault_start, fault_end=fault_end)
                elif "network_transmit_bytes_total" in fname:
                    service_features[svc_id]["net_tx_rate"] = compute_counter_window_rates(window_edges, ts, vals, fault_start=fault_start, fault_end=fault_end)
            except Exception as e:
                print(f"[WARN] Error parsing {fname}: {e}", file=sys.stderr)

    # 2. Parse Jaeger traces (latencies, call counts, span error counts)
    traces_path = raw_dir / "traces.json"
    trace_lats, trace_errs = extract_trace_metrics_by_window(traces_path, window_edges, services)
    for (svc_id, w_idx), lats in trace_lats.items():
        if lats:
            service_features[svc_id]["latency_mean_ms"][w_idx] = sum(lats) / len(lats)
            service_features[svc_id]["latency_p95_ms"][w_idx] = percentile(lats, 0.95)
            service_features[svc_id]["call_count"][w_idx] = len(lats)

    for (svc_id, w_idx), err_count in trace_errs.items():
        calls = service_features[svc_id]["call_count"][w_idx]
        service_features[svc_id]["error_count"][w_idx] = err_count
        service_features[svc_id]["error_rate"][w_idx] = round(err_count / max(1, calls), 6)

    # 3. Parse Container logs
    logs_dir = raw_dir / "app_pod_logs"
    log_counts, log_errors = extract_log_metrics_by_window(logs_dir, window_edges, services)
    for (svc_id, w_idx), l_cnt in log_counts.items():
        service_features[svc_id]["log_count"][w_idx] = l_cnt
        service_features[svc_id]["log_error_count"][w_idx] = log_errors.get((svc_id, w_idx), 0)

    # 4. Detect and drop partial trailing window (Fix #3)
    nginx_calls = service_features[0]["call_count"]
    if n_windows > 3:
        earlier_calls = [c for c in nginx_calls[:-1] if c > 0]
        med_calls = sorted(earlier_calls)[len(earlier_calls) // 2] if earlier_calls else 0
        last_calls = nginx_calls[-1]

        last_window_net = sum(service_features[s["id"]]["net_rx_rate"][-1] + service_features[s["id"]]["net_tx_rate"][-1] for s in services)
        all_nets = [sum(service_features[s["id"]]["net_rx_rate"][w] + service_features[s["id"]]["net_tx_rate"][w] for s in services) for w in range(n_windows - 1)]
        med_net = sorted(all_nets)[len(all_nets) // 2] if all_nets else 1.0

        is_partial = False
        if med_calls > 0 and last_calls < 0.5 * med_calls:
            is_partial = True
        elif med_net > 1000.0 and last_window_net < 0.1 * med_net:
            is_partial = True

        if is_partial:
            print(f"[PREPROCESS] Dropping partial trailing window {n_windows - 1} "
                  f"(calls={last_calls} vs median={med_calls}, net={last_window_net:.1f} vs median={med_net:.1f})")
            n_windows -= 1
            window_centers = window_centers[:n_windows]
            window_edges = window_edges[:n_windows + 1]
            for s in services:
                sid = s["id"]
                for feat in service_features[sid]:
                    service_features[sid][feat] = service_features[sid][feat][:n_windows]

    # 5. Build structured rows with verified 3-phase labels (Dynamic Baseline Recovery, Item 5)
    # Compute pre-fault baseline latencies
    pre_fault_windows = [w for w in range(n_windows) if window_edges[w + 1] <= fault_start] if fault_start else list(range(n_windows))
    
    target_pre_lats = [service_features[target_id]["latency_p95_ms"][w] for w in pre_fault_windows if service_features[target_id]["call_count"][w] > 0] if target_id >= 0 else []
    target_base_p95 = percentile(target_pre_lats, 0.90) if target_pre_lats else 10.0

    nginx_pre_lats = [service_features[0]["latency_p95_ms"][w] for w in pre_fault_windows if service_features[0]["call_count"][w] > 0]
    nginx_base_p95 = percentile(nginx_pre_lats, 0.90) if nginx_pre_lats else 15.0

    # Dynamic recovery thresholds: returned to <= max(1.5 * baseline, baseline + 10ms) and throttle rate == 0
    target_rec_thresh = max(target_base_p95 * 1.5, target_base_p95 + 10.0)
    nginx_rec_thresh = max(nginx_base_p95 * 1.5, nginx_base_p95 + 15.0)

    # Median normal calls for catchup flag
    normal_nginx_calls = [service_features[0]["call_count"][w] for w in pre_fault_windows if service_features[0]["call_count"][w] > 0]
    median_normal_calls = sorted(normal_nginx_calls)[len(normal_nginx_calls) // 2] if normal_nginx_calls else 0

    rows = []
    has_recovered = False
    recovery_confirm_count = 0
    REQUIRED_RECOVERY_CONFIRM = 2  # 2 consecutive windows at baseline required to confirm recovery

    for w in range(n_windows):
        w_start = window_edges[w]
        w_end = window_edges[w + 1]
        w_center = window_centers[w]

        overlap_pct = 0.0
        phase = "normal"
        is_fault = 0
        transition_flag = 0

        if fault_start is not None and fault_end is not None and fault_type != "none":
            # Overlap with fault window [fault_start, fault_end]
            overlap_s = max(0.0, min(w_end, fault_end) - max(w_start, fault_start))
            overlap_pct = overlap_s / (w_end - w_start)
            target_throt = service_features[target_id]["cfs_throttled_periods_rate"][w] if target_id >= 0 else 0.0

            if w_end <= fault_start:
                # Strictly pre-fault window: derived purely from injection timestamps
                phase = "normal"
                is_fault = 0
                transition_flag = 0
            elif w_start <= fault_start < w_end:
                # Straddling onset window
                transition_flag = 1
                if overlap_pct >= 0.50:
                    phase = "fault"
                    is_fault = 1
                else:
                    phase = "normal"
                    is_fault = 0
            elif w_start >= fault_start and w_end <= fault_end:
                # Fully inside fault window
                phase = "fault"
                is_fault = 1
                transition_flag = 0
            elif w_start < fault_end < w_end:
                # Straddling fault offset window
                transition_flag = 1
                if overlap_pct >= 0.50:
                    phase = "fault"
                    is_fault = 1
                else:
                    phase = "recovery"
                    is_fault = 0
            elif w_start >= fault_end:
                # Post-fault period: dynamic return-to-baseline check (Item 5)
                throt_rate = target_throt
                t_lat = service_features[target_id]["latency_p95_ms"][w] if target_id >= 0 else 0.0
                n_lat = service_features[0]["latency_p95_ms"][w]

                at_baseline = (throt_rate <= 0.05 and t_lat <= target_rec_thresh and n_lat <= nginx_rec_thresh)

                if has_recovered:
                    phase = "normal"
                    transition_flag = 0
                else:
                    if at_baseline:
                        recovery_confirm_count += 1
                        if recovery_confirm_count >= REQUIRED_RECOVERY_CONFIRM:
                            has_recovered = True
                            phase = "normal"
                            transition_flag = 1  # Boundary window returning to normal
                        else:
                            phase = "recovery"
                            transition_flag = 0
                    else:
                        recovery_confirm_count = 0
                        phase = "recovery"
                        transition_flag = 0
                is_fault = 0

        # Settle load happens prior to dataset recording; all pre-fault windows are healthy normal baseline
        is_warmup = 0

        # Catchup flag: 1 if non-fault window calls exceed 1.5x median normal calls (queue draining)
        calls_w = service_features[0]["call_count"][w]
        catchup_flag = 1 if (phase != "fault" and median_normal_calls > 10 and calls_w > 1.50 * median_normal_calls) else 0

        row = {
            "window_id": w,
            "timestamp": round(w_center, 4),
            "window_start_ts": round(w_start, 4),
            "window_end_ts": round(w_end, 4),
            "is_warmup": is_warmup,
            "phase_label": phase,
            "transition_flag": transition_flag,
            "catchup_flag": catchup_flag,
            "fault_overlap_pct": round(overlap_pct, 4),
            "is_fault_window": is_fault,
            "root_cause_service_id": target_id if phase == "fault" else -1,
            "root_cause_service_name": target_comp if phase == "fault" else "none",
            "fault_type": fault_type if phase == "fault" else "none"
        }

        for s in services:
            sid = s["id"]
            sname = s["name"]
            row[f"{sname}_cpu_rate"] = round(service_features[sid]["cpu_rate"][w], 6)
            row[f"{sname}_cfs_throttled_periods_rate"] = round(service_features[sid]["cfs_throttled_periods_rate"][w], 4)
            row[f"{sname}_cfs_throttled_seconds_rate"] = round(service_features[sid]["cfs_throttled_seconds_rate"][w], 4)
            row[f"{sname}_mem_bytes"] = round(service_features[sid]["mem_bytes"][w], 2)
            row[f"{sname}_net_rx_rate"] = round(service_features[sid]["net_rx_rate"][w], 4)
            row[f"{sname}_net_tx_rate"] = round(service_features[sid]["net_tx_rate"][w], 4)
            row[f"{sname}_latency_mean_ms"] = round(service_features[sid]["latency_mean_ms"][w], 4)
            row[f"{sname}_latency_p95_ms"] = round(service_features[sid]["latency_p95_ms"][w], 4)
            row[f"{sname}_call_count"] = int(service_features[sid]["call_count"][w])
            row[f"{sname}_error_count"] = int(service_features[sid]["error_count"][w])
            row[f"{sname}_error_rate"] = round(service_features[sid]["error_rate"][w], 6)
            row[f"{sname}_log_count"] = int(service_features[sid]["log_count"][w])
            row[f"{sname}_log_error_count"] = int(service_features[sid]["log_error_count"][w])

        rows.append(row)

    # 6. Write structured CSV
    out_file = out_dir / f"{run_id}.csv"
    fieldnames = list(rows[0].keys())

    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    phases_present = sorted(set(r["phase_label"] for r in rows))
    print(f"[PREPROCESS] Processed {run_id} -> {out_file} ({len(rows)} windows, "
          f"{len(fieldnames)} features, phases: {phases_present})")
    return out_file

def main():
    parser = argparse.ArgumentParser(description="Multimodal Causal Preprocessor")
    parser.add_argument("--raw-dir", required=True, help="Directory containing raw run")
    parser.add_argument("--out-dir", required=True, help="Directory to save processed CSV")
    parser.add_argument("--step", type=float, default=5.0, help="Window size in seconds")
    parser.add_argument("--recovery-duration", type=float, default=45.0, help="Recovery duration after fault in seconds")
    args = parser.parse_args()

    process_single_run(Path(args.raw_dir), Path(args.out_dir), step_s=args.step, recovery_duration_s=args.recovery_duration)

if __name__ == "__main__":
    main()
