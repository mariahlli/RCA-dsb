#!/usr/bin/env python3
"""
scripts/workload/run_workload.py
Runs reproducible load against DeathStarBench using the containerized wrk2 generator:
  Endpoints supported:
    - compose: /wrk2-api/post/compose (via compose-post.lua)
    - home: /wrk2-api/home-timeline/read (via read-home-timeline.lua)
    - user: /wrk2-api/user-timeline/read (via read-user-timeline.lua)
    - mixed: mixed-workload.lua
"""
import os
import sys
import re
import argparse
import subprocess
import json

LUA_DIR = "/home/maria/DeathStarBench/socialNetwork/wrk2/scripts/social-network"

SCRIPT_MAP = {
    "compose": ("compose-post.lua", "http://localhost:8080/wrk2-api/post/compose"),
    "home": ("read-home-timeline.lua", "http://localhost:8080/wrk2-api/home-timeline/read"),
    "user": ("read-user-timeline.lua", "http://localhost:8080/wrk2-api/user-timeline/read"),
    "mixed": ("mixed-workload.lua", "http://localhost:8080")
}

def parse_wrk2_output(stdout_str):
    summary = {}
    
    # Requests / sec
    m_req = re.search(r"Requests/sec:\s+([\d.]+)", stdout_str)
    if m_req:
        summary["actual_rps"] = float(m_req.group(1))

    # Latencies
    m_p50 = re.search(r"50\.000%\s+([\d.]+)(us|ms|s)", stdout_str)
    m_p90 = re.search(r"90\.000%\s+([\d.]+)(us|ms|s)", stdout_str)
    m_p99 = re.search(r"99\.000%\s+([\d.]+)(us|ms|s)", stdout_str)
    
    def to_ms(val, unit):
        v = float(val)
        if unit == "us": return v / 1000.0
        elif unit == "ms": return v
        elif unit == "s": return v * 1000.0
        return v

    if m_p50: summary["p50_latency_ms"] = to_ms(m_p50.group(1), m_p50.group(2))
    if m_p90: summary["p90_latency_ms"] = to_ms(m_p90.group(1), m_p90.group(2))
    if m_p99: summary["p99_latency_ms"] = to_ms(m_p99.group(1), m_p99.group(2))
    
    # Non-2xx/3xx responses
    m_err = re.search(r"Non-2xx or 3xx responses:\s+(\d+)", stdout_str)
    summary["non_2xx_responses"] = int(m_err.group(1)) if m_err else 0

    return summary

def main():
    parser = argparse.ArgumentParser(description="Run wrk2 load against DeathStarBench")
    parser.add_argument("--endpoint", choices=list(SCRIPT_MAP.keys()), default="compose")
    parser.add_argument("--rps", type=int, default=100, help="Target requests per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--connections", type=int, default=32, help="wrk2 connections")
    parser.add_argument("--threads", type=int, default=8, help="wrk2 threads")
    parser.add_argument("--host-network", action="store_true", default=True, help="Use host network")
    args = parser.parse_args()

    lua_script, url = SCRIPT_MAP[args.endpoint]
    
    # Use containerized rca-wrk2 with host network
    cmd = [
        "docker", "run", "--rm",
        "--net=host",
        "-v", f"{LUA_DIR}:/scripts:ro",
        "rca-wrk2:latest",
        "/usr/local/bin/wrk",
        f"-D", "exp",
        f"-t{args.threads}",
        f"-c{args.connections}",
        f"-d{args.duration}s",
        "-L",
        f"-s", f"/scripts/{lua_script}",
        url,
        f"-R{args.rps}"
    ]

    print(f"[WORKLOAD] Starting wrk2 against {args.endpoint} ({url}) at {args.rps} RPS for {args.duration}s...")
    res = subprocess.run(cmd, capture_output=True, text=True)
    
    if res.returncode != 0:
        print(f"[ERROR] wrk2 failed:\n{res.stderr}", file=sys.stderr)
        sys.exit(res.returncode)

    print("[WORKLOAD] wrk2 execution finished.")
    summary = parse_wrk2_output(res.stdout)
    summary["target_rps"] = args.rps
    summary["endpoint"] = args.endpoint
    summary["duration_s"] = args.duration
    summary["raw_output"] = res.stdout

    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
