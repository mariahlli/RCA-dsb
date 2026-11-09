#!/usr/bin/env python3
"""
scripts/orchestration/run_pilots.py
Sequential Pilot Campaign Runner with Automatic Audit Verification.

Executes pilot runs in order, audits each run with verify_run.py immediately upon completion,
and halts on any failure (Rule R2, R3).
"""

import os
import sys
import time
import json
import subprocess
import argparse
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", str(Path.home() / "rca-project")))
EXPERIMENT_SCRIPT = PROJECT_ROOT / "scripts" / "orchestration" / "run_experiment.py"
VERIFIER_SCRIPT = PROJECT_ROOT / "scripts" / "audit" / "verify_run.py"
OUTPUT_DIR = PROJECT_ROOT / "results" / "new_dataset"

PILOT_CONFIGS = [
    # ─── Baselines ───────────────────────────────────────────
    {
        "run_id": "pilot_baseline_compose_50rps_300s",
        "endpoint": "compose",
        "rps": 50,
        "duration": 300,
        "warm_up": 60,
        "fault_type": "none",
        "seed": 42
    },
    {
        "run_id": "pilot_baseline_home_50rps_300s",
        "endpoint": "home",
        "rps": 50,
        "duration": 300,
        "warm_up": 60,
        "fault_type": "none",
        "seed": 42
    },
    {
        "run_id": "pilot_baseline_mixed_50rps_300s",
        "endpoint": "mixed",
        "rps": 50,
        "duration": 300,
        "warm_up": 60,
        "fault_type": "none",
        "seed": 42
    },
    {
        "run_id": "pilot_baseline_compose_200rps_120s",
        "endpoint": "compose",
        "rps": 200,
        "duration": 120,
        "warm_up": 60,
        "fault_type": "none",
        "seed": 42
    },
    {
        "run_id": "pilot_baseline_home_200rps_120s",
        "endpoint": "home",
        "rps": 200,
        "duration": 120,
        "warm_up": 60,
        "fault_type": "none",
        "seed": 42
    },
    # ─── Fault Runs (180s: 60s pre-fault, 30s fault, 90s recovery) ───
    {
        "run_id": "pilot_cpu_throttle_compose_25rps_rep1",
        "endpoint": "compose",
        "rps": 25,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "cpu",
        "cpu_mode": "throttle",
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "fault_intensity": 0.04,
        "seed": 42
    },
    {
        "run_id": "pilot_cpu_throttle_compose_25rps_rep2",
        "endpoint": "compose",
        "rps": 25,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "cpu",
        "cpu_mode": "throttle",
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "fault_intensity": 0.04,
        "seed": 43
    },
    {
        "run_id": "pilot_cpu_throttle_compose_200rps",
        "endpoint": "compose",
        "rps": 200,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "cpu",
        "cpu_mode": "throttle",
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "fault_intensity": 0.04,
        "seed": 42
    },
    {
        "run_id": "pilot_net_delay_compose_200rps",
        "endpoint": "compose",
        "rps": 200,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "network",
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "delay_ms": 50,
        "loss_pct": 0.0,
        "seed": 42
    },
    {
        "run_id": "pilot_net_loss_compose_25rps",
        "endpoint": "compose",
        "rps": 25,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "network",
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "delay_ms": 0,
        "loss_pct": 15.0,
        "seed": 42
    },
    {
        "run_id": "pilot_mem_compose_200rps",
        "endpoint": "compose",
        "rps": 200,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "memory",
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "fault_intensity": 75,
        "seed": 42
    },
    {
        "run_id": "pilot_cpu_burn_compose_50rps",
        "endpoint": "compose",
        "rps": 50,
        "duration": 180,
        "warm_up": 60,
        "fault_type": "cpu",
        "cpu_mode": "burn",
        "burn_cores": 2,
        "fault_target": "compose-post-service",
        "fault_duration": 30,
        "fault_intensity": 2.0,
        "seed": 42
    }
]

def run_single_pilot(cfg):
    run_id = cfg["run_id"]
    endpoint = cfg["endpoint"]
    processed_csv = OUTPUT_DIR / "processed_dataset" / endpoint / f"{run_id}.csv"
    args_file = OUTPUT_DIR / "raw_dataset" / run_id / "args.txt"

    # Check if already completed and valid
    if processed_csv.exists() and args_file.exists():
        print(f"[RUNNER] Checking existing output for {run_id}...")
        res = subprocess.run([
            sys.executable, str(VERIFIER_SCRIPT),
            "--csv", str(processed_csv),
            "--args", str(args_file)
        ], capture_output=True, text=True)
        is_pass = False
        try:
            val = json.loads(res.stdout)
            is_pass = (val.get("status") == "PASS")
        except Exception:
            is_pass = ("[FAIL]" not in res.stdout and res.returncode == 0)
        if is_pass:
            print(f"[RUNNER] Run {run_id} ALREADY COMPLETED AND VALIDATED. Skipping.")
            return True

    cmd = [
        sys.executable, str(EXPERIMENT_SCRIPT),
        "--run-id", run_id,
        "--endpoint", endpoint,
        "--rps", str(cfg["rps"]),
        "--duration", str(cfg["duration"]),
        "--warm-up", str(cfg["warm_up"]),
        "--fault-type", cfg.get("fault_type", "none"),
        "--seed", str(cfg.get("seed", 42)),
        "--out-dir", str(OUTPUT_DIR)
    ]
    if cfg.get("fault_target"):
        cmd.extend(["--fault-target", cfg["fault_target"]])
    if "fault_duration" in cfg:
        cmd.extend(["--fault-duration", str(cfg["fault_duration"])])
    if "fault_intensity" in cfg:
        cmd.extend(["--fault-intensity", str(cfg["fault_intensity"])])
    if "cpu_mode" in cfg:
        cmd.extend(["--cpu-mode", cfg["cpu_mode"]])
    if "burn_cores" in cfg:
        cmd.extend(["--burn-cores", str(cfg["burn_cores"])])
    if "delay_ms" in cfg:
        cmd.extend(["--delay-ms", str(cfg["delay_ms"])])
    if "loss_pct" in cfg:
        cmd.extend(["--loss-pct", str(cfg["loss_pct"])])

    print(f"\n========================================================")
    print(f" [RUNNER] EXECUTING PILOT: {run_id}")
    print(f" Command: {' '.join(cmd)}")
    print(f"========================================================\n")

    res = subprocess.run(cmd)
    if res.returncode != 0:
        print(f"[ERROR] Run {run_id} failed with exit code {res.returncode}", file=sys.stderr)
        return False

    # Audit with verify_run.py
    print(f"\n[RUNNER] AUDITING RUN: {run_id} with verify_run.py...")
    audit_res = subprocess.run([
        sys.executable, str(VERIFIER_SCRIPT),
        "--csv", str(processed_csv),
        "--args", str(args_file)
    ], capture_output=True, text=True)
    print(audit_res.stdout)
    is_audit_pass = False
    try:
        val = json.loads(audit_res.stdout)
        is_audit_pass = (val.get("status") == "PASS")
    except Exception:
        is_audit_pass = (audit_res.returncode == 0 and "[FAIL]" not in audit_res.stdout)
    if not is_audit_pass:
        print(f"[FATAL] Audit Gate FAILED for {run_id}!", file=sys.stderr)
        return False

    # 15s post-run cooldown
    print(f"[RUNNER] Run {run_id} succeeded. Cooldown for 15s...\n")
    time.sleep(15)
    return True

def main():
    parser = argparse.ArgumentParser(description="Pilot Campaign Runner")
    parser.add_argument("--run-id", help="Execute specific run_id only")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of runs")
    args = parser.parse_args()

    configs_to_run = PILOT_CONFIGS
    if args.run_id:
        configs_to_run = [c for c in PILOT_CONFIGS if c["run_id"] == args.run_id]
        if not configs_to_run:
            print(f"[ERROR] Unknown run_id: {args.run_id}", file=sys.stderr)
            sys.exit(1)

    if args.limit:
        configs_to_run = configs_to_run[:args.limit]

    print(f"[RUNNER] Starting pilot campaign: {len(configs_to_run)} runs scheduled.")
    for idx, cfg in enumerate(configs_to_run, 1):
        print(f"\n>>> Running pilot {idx}/{len(configs_to_run)}: {cfg['run_id']}")
        ok = run_single_pilot(cfg)
        if not ok:
            print(f"[FATAL] Pilot campaign HALTED at {cfg['run_id']} due to error or audit failure.", file=sys.stderr)
            sys.exit(1)

    print("\n========================================================")
    print(f" ALL {len(configs_to_run)} PILOT RUNS COMPLETED AND VALIDATED.")
    print("========================================================\n")

if __name__ == "__main__":
    main()
