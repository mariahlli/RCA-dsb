#!/usr/bin/env python3
"""
scripts/collector/export_telemetry.py
Queries Prometheus, Jaeger, and Docker daemon to dump high-resolution multimodal telemetry:
  1. Prometheus metric range queries -> prom_metrics/<service>_<metric>
     Includes CFS throttling metrics (throttled_periods, throttled_seconds)
  2. Jaeger distributed traces -> traces.json
  3. Docker container logs (with --timestamps) -> app_pod_logs/<service>.log
"""
import os
import sys
import json
import time
import argparse
import urllib.request
import urllib.parse
import subprocess
from pathlib import Path

METRIC_FAMILIES = [
    "container_cpu_usage_seconds_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_cpu_cfs_throttled_seconds_total",
    "container_memory_usage_bytes",
    "container_memory_working_set_bytes",
    "container_network_receive_bytes_total",
    "container_network_transmit_bytes_total",
    "container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total"
]

def get_docker_container_map():
    mapping = {}
    try:
        res = subprocess.run("docker ps -a --no-trunc --format '{{.ID}}\t{{.Names}}'",
                             shell=True, capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if "\t" in line:
                cid, cname = line.split("\t", 1)
                cid = cid.strip()
                cname = cname.strip()
                clean = cname.replace("socialnetwork-", "").replace("social-network-", "")
                if clean.endswith("-1"):
                    clean = clean[:-2]
                mapping[cid] = clean
                mapping[cid[:12]] = clean
                mapping[cname] = clean
    except Exception as e:
        print(f"[WARN] Failed to get docker container map: {e}", file=sys.stderr)
    return mapping

def query_prometheus_range(prom_url, query, start_ts, end_ts, step="5s"):
    params = {
        "query": query,
        "start": start_ts,
        "end": end_ts,
        "step": step
    }
    url = f"{prom_url}/api/v1/query_range?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"[WARN] Prometheus query failed for {query}: {e}", file=sys.stderr)
    return []

def query_jaeger_traces(jaeger_url, service, start_ts, end_ts, chunk_s=30.0, limit_per_chunk=50000):
    """
    Query Jaeger traces paginated by time window to guarantee 100% trace capture
    without hitting any internal API trace limits.
    """
    start_us = int(start_ts * 1_000_000)
    end_us = int(end_ts * 1_000_000)
    step_us = int(chunk_s * 1_000_000)

    all_traces = []
    seen_trace_ids = set()

    curr_start = start_us
    while curr_start < end_us:
        curr_end = min(end_us, curr_start + step_us)
        params = {
            "service": service,
            "start": curr_start,
            "end": curr_end,
            "limit": limit_per_chunk
        }
        url = f"{jaeger_url}/api/traces?{urllib.parse.urlencode(params)}"
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                traces = data.get("data", [])
                for tr in traces:
                    tid = tr.get("traceID")
                    if tid and tid not in seen_trace_ids:
                        seen_trace_ids.add(tid)
                        all_traces.append(tr)
        except Exception as e:
            print(f"[WARN] Jaeger traces query failed for window [{curr_start}..{curr_end}]: {e}", file=sys.stderr)
        curr_start = curr_end

    return all_traces

def collect_docker_logs(target_dir, start_ts, end_ts):
    """
    Collect Docker container logs with --timestamps flag for reliable
    per-line timestamps (ISO 8601 prefix added by Docker daemon).
    This enables window-aligned log parsing in process_run.py.
    """
    logs_dir = Path(target_dir) / "app_pod_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    res = subprocess.run("docker ps -a --format '{{.Names}}'", shell=True, capture_output=True, text=True)
    containers = [c.strip() for c in res.stdout.splitlines() if c.strip()]
    for c in containers:
        # --timestamps adds Docker's own ISO 8601 timestamp prefix per line
        cmd = f"docker logs --timestamps --since {int(start_ts)} --until {int(end_ts)} {c}"
        log_res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if log_res.stdout or log_res.stderr:
            out_file = logs_dir / f"{c}.log"
            out_file.write_text(log_res.stdout + ("\n--- STDERR ---\n" + log_res.stderr if log_res.stderr else ""))

def main():
    parser = argparse.ArgumentParser(description="Multimodal Telemetry Exporter")
    parser.add_argument("--out", required=True, help="Destination directory for raw run")
    parser.add_argument("--start", type=float, required=True, help="Start epoch timestamp")
    parser.add_argument("--end", type=float, required=True, help="End epoch timestamp")
    parser.add_argument("--prom-url", default="http://localhost:9090", help="Prometheus base URL")
    parser.add_argument("--jaeger-url", default="http://localhost:16686", help="Jaeger base URL")
    parser.add_argument("--step", default="5s", help="Prometheus sampling step")
    args = parser.parse_args()

    out_dir = Path(args.out)
    prom_dir = out_dir / "prom_metrics"
    prom_dir.mkdir(parents=True, exist_ok=True)

    print(f"[COLLECTOR] Exporting telemetry for window [{args.start:.2f} -> {args.end:.2f}] (dur: {args.end - args.start:.1f}s)")

    cid_map = get_docker_container_map()

    # 1. Prometheus metrics (query with 15s pre-window margin and 2s sampling step)
    prom_query_start = args.start - 15.0
    prom_query_end = args.end + 5.0
    prom_step = "2s" if args.step == "5s" else args.step

    for metric in METRIC_FAMILIES:
        results = query_prometheus_range(args.prom_url, metric, prom_query_start, prom_query_end, step=prom_step)
        for series in results:
            metric_meta = series.get("metric", {})
            raw_id = (metric_meta.get("name") or
                      metric_meta.get("container") or
                      metric_meta.get("container_label_io_kubernetes_container_name") or
                      metric_meta.get("id", "unknown"))
            cid = raw_id.split("/")[-1]
            svc_name = cid_map.get(cid, cid)
            if not svc_name or svc_name.startswith("system.") or svc_name.startswith("user.") or svc_name.startswith("netshoot") or svc_name in ["cadvisor", "prometheus", "/"]:
                continue
            values = series.get("values", [])
            out_path = prom_dir / f"{svc_name}_{metric}"
            # Preserve network sampler series if Prometheus has no data
            if "network_" in metric and out_path.exists() and not values:
                continue
            # Only overwrite if values non-empty or file doesn't exist
            if values or not out_path.exists():
                out_path.write_text(json.dumps(values, indent=2))

    # 2. Jaeger traces
    print("[COLLECTOR] Fetching Jaeger traces...")
    traces = query_jaeger_traces(args.jaeger_url, "nginx-web-server", args.start, args.end)
    traces_file = out_dir / "traces.json"
    traces_file.write_text(json.dumps(traces))
    print(f"[COLLECTOR] Exported {len(traces)} traces to {traces_file}")

    # 3. Container logs (with timestamps for window alignment)
    print("[COLLECTOR] Exporting container logs (with --timestamps)...")
    collect_docker_logs(out_dir, args.start, args.end)

    print("[COLLECTOR] Telemetry export complete.")

if __name__ == "__main__":
    main()
