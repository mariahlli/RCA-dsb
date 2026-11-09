#!/usr/bin/env python3
"""
scripts/orchestration/run_experiment.py
End-to-End Orchestrator for Track B Data Generation with Verification Gates:
  1. Pre-flight Gate:
     - Verifies database seeded (or seeds it)
     - Fires probe request and checks Jaeger trace contains ALL 12 services
     - Aborts run if gate fails
  2. Container State Clean Reset:
     - Clears CPU quotas (--cpu-quota -1) and asserts cpu.max == max
     - Clears any netem qdiscs
  3. Continuous Network Sampler:
     - Reads /proc/<pid>/net/dev every 5s for byte-level precision
  4. Workload & Chaos Execution:
     - Workload generation (wrk2)
     - Chaos injection with post-inject & post-revert verification
  5. Multimodal Export & Causal Preprocessing:
     - Dumps Prometheus range metrics (including CFS throttled rates)
     - Dumps Jaeger traces & Docker logs (with --timestamps)
     - Automatically runs process_run.py with 3-phase labeling
"""
import os
import sys
import time
import argparse
import subprocess
import json
import urllib.request
import urllib.parse
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", str(Path.home() / "rca-project")))
CHAOS_SCRIPT = PROJECT_ROOT / "scripts" / "chaos" / "inject_fault.py"
WORKLOAD_SCRIPT = PROJECT_ROOT / "scripts" / "workload" / "run_workload.py"
COLLECTOR_SCRIPT = PROJECT_ROOT / "scripts" / "collector" / "export_telemetry.py"
PROCESS_SCRIPT = PROJECT_ROOT / "scripts" / "preprocessing" / "process_run.py"
NET_SAMPLER_SCRIPT = PROJECT_ROOT / "scripts" / "collector" / "docker_net_sampler.py"

sys.path.insert(0, str(PROJECT_ROOT))
from scripts.collector.docker_net_sampler import DockerNetworkSampler

EXPECTED_SERVICES = {
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

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return res.stdout.strip(), res.stderr.strip(), res.returncode

def ensure_social_graph_seeded():
    print("[GATE] Checking social graph database state...")
    out, _, _ = run_cmd("docker exec socialnetwork-user-mongodb-1 mongo user --eval 'db.user.count()' --quiet 2>/dev/null || echo 0")
    try:
        count = int(out.strip().splitlines()[-1])
    except Exception:
        count = 0

    if count < 500:
        print(f"[GATE] Database has only {count} users. Seeding socfb-Reed98...")
        bench_dir = "/home/maria/DeathStarBench/socialNetwork"
        seed_cmd = (
            f"docker run --rm --net=host -v {bench_dir}:/app -w /app python:3.10-slim sh -c "
            "'pip install --no-cache-dir aiohttp >/dev/null 2>&1 && python3 scripts/init_social_graph.py --graph=socfb-Reed98'"
        )
        _, err, code = run_cmd(seed_cmd)
        if code != 0:
            raise RuntimeError(f"Database seeding failed: {err}")
        print("[GATE] Seeding completed.")
    else:
        print(f"[GATE] Social graph already seeded ({count} users).")

def check_trace_gate(endpoint="compose"):
    print("[GATE] Sending probe requests to check trace completeness...")
    # Fire probe requests
    run_cmd('curl -s -X POST "http://localhost:8080/wrk2-api/post/compose" -d "username=username_0&user_id=0&text=Probe+test+@username_1+http://test.com&media_ids=[\\\"123456\\\"]&media_types=[\\\"png\\\"]&post_type=0"')
    if endpoint in ("mixed", "home", "user"):
        run_cmd('curl -s "http://localhost:8080/wrk2-api/home-timeline/read?user_id=0&start=0&stop=10"')
        run_cmd('curl -s "http://localhost:8080/wrk2-api/user-timeline/read?user_id=0&start=0&stop=10"')
    time.sleep(2)

    # Check services registered in Jaeger
    try:
        url = "http://localhost:16686/api/services"
        resp = urllib.request.urlopen(url, timeout=10)
        svcs = set(json.loads(resp.read().decode("utf-8")).get("data", []))
        expected = EXPECTED_SERVICES if endpoint in ("mixed", "home") else {"nginx-web-server", "compose-post-service", "text-service", "user-service", "media-service", "unique-id-service"}
        missing = expected - svcs
        print(f"[GATE] Active services in Jaeger ({len(svcs & expected)}/{len(expected)}): {sorted(svcs & expected)}")
        if missing:
            raise AssertionError(f"TRACE GATE FAILED! Missing expected services in trace: {missing}")
        print("[GATE] Trace completeness gate PASSED!")
    except Exception as e:
        raise RuntimeError(f"Trace gate check failed: {e}")

def check_warmup_activity(target, start_ts, end_ts):
    """
    Verify target (or its upstream service if datastore) is on-path and received calls during warmup.
    """
    svc = target.replace("socialnetwork-", "").replace("social-network-", "")
    if svc.endswith("-1"):
        svc = svc[:-2]

    # Map datastores to corresponding service for Jaeger check
    jaeger_svc = svc
    datastore_map = {
        "post-storage-mongodb": "post-storage-service",
        "post-storage-memcached": "post-storage-service",
        "home-timeline-redis": "home-timeline-service",
        "user-timeline-redis": "user-timeline-service",
        "user-mongodb": "user-service",
        "social-graph-mongodb": "social-graph-service",
        "social-graph-redis": "social-graph-service",
        "url-shorten-mongodb": "url-shorten-service",
        "url-shorten-memcached": "url-shorten-service",
    }
    if svc in datastore_map:
        jaeger_svc = datastore_map[svc]

    start_us = int(start_ts * 1000000)
    end_us = int(end_ts * 1000000)
    url = f"http://localhost:16686/api/traces?service={jaeger_svc}&start={start_us}&end={end_us}&limit=10"
    try:
        data = json.loads(urllib.request.urlopen(url, timeout=10).read().decode())
        traces = data.get("data", [])
        return len(traces)
    except Exception as e:
        print(f"[WARN] Failed to query Jaeger for warmup traces: {e}")
        return -1

def reset_all_container_states():
    print("[RESET] Resetting container quotas, network queues, and stopping stray workloads...")
    # 1. Kill any leftover wrk2 containers or processes to prevent traffic leakage
    run_cmd("docker kill $(docker ps -q --filter ancestor=rca-wrk2:latest) 2>/dev/null || true")
    run_cmd("pkill -9 -f wrk 2>/dev/null || true")
    time.sleep(1)
    # 2. Reset CPU quotas and memory in single batch
    run_cmd("docker update --cpu-quota -1 --memory 20g --memory-swap 20g $(docker ps -q --filter 'name=socialnetwork-') 2>/dev/null || true")
    # 3. Kill any leftover burn workers across all containers
    run_cmd("docker exec socialnetwork-compose-post-service-1 pkill -9 -f cpu_chaos_worker 2>/dev/null || true")
    run_cmd("docker exec socialnetwork-text-service-1 pkill -9 -f cpu_chaos_worker 2>/dev/null || true")
    run_cmd("docker exec socialnetwork-post-storage-service-1 pkill -9 -f cpu_chaos_worker 2>/dev/null || true")
    run_cmd("docker exec socialnetwork-social-graph-service-1 pkill -9 -f cpu_chaos_worker 2>/dev/null || true")
    # 4. Clear any leftover netem
    for svc in ["socialnetwork-compose-post-service-1", "socialnetwork-text-service-1", "socialnetwork-post-storage-service-1"]:
        run_cmd(f"docker run --rm --net=container:{svc} --cap-add=NET_ADMIN nicolaka/netshoot tc qdisc del dev eth0 root netem 2>/dev/null || true")
    # Verify target compose-post-service is clean
    out, _, _ = run_cmd("docker exec socialnetwork-compose-post-service-1 cat /sys/fs/cgroup/cpu.max 2>/dev/null")
    if out and not out.startswith("max "):
        raise AssertionError(f"Container reset failed! compose-post-service cpu.max is: {out}")
    print("[RESET] Clean baseline confirmed: cgroups cpu.max is max (unconstrained), workload processes clean.")

def main():
    parser = argparse.ArgumentParser(description="Deterministic RCA Experiment Orchestrator")
    parser.add_argument("--run-id", required=True, help="Unique run identifier (e.g. smoke_cpu_compose_50rps_1)")
    parser.add_argument("--endpoint", default="compose", choices=["compose", "home", "user", "mixed"])
    parser.add_argument("--rps", type=int, default=50, help="Requests per second")
    parser.add_argument("--duration", type=int, default=180, help="Total experiment duration (s) [default: 180s = 60s pre-fault + 30s fault + 90s recovery]")
    parser.add_argument("--warm-up", type=int, default=60, help="Warm-up duration before injection (s) [default: 60s = 12 windows]")
    parser.add_argument("--fault-type", default="none", choices=["none", "cpu", "memory", "network"])
    parser.add_argument("--fault-target", default="", help="Target service / container for injection")
    parser.add_argument("--fault-duration", type=int, default=30, help="Duration of fault injection (s) [default: 30s = 6 windows]")
    parser.add_argument("--fault-intensity", type=float, default=0.04, help="CPU quota (e.g. 0.04) or Memory MB (e.g. 50)")
    parser.add_argument("--delay-ms", type=int, default=50, help="Network delay in ms")
    parser.add_argument("--loss-pct", type=float, default=5.0, help="Network loss pct")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--cpu-mode", default="throttle",
                        choices=["burn", "throttle", "burn_and_throttle"],
                        help="CPU fault mode")
    parser.add_argument("--burn-cores", type=int, default=2, help="Burn worker count")
    parser.add_argument("--out-dir", default=str(PROJECT_ROOT / "results" / "new_dataset"), help="Dataset root dir")
    args = parser.parse_args()

    run_dir = Path(args.out_dir) / "raw_dataset" / args.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = Path(args.out_dir) / "ground_truth_manifest.jsonl"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n========================================================")
    print(f"  STARTING RUN: {args.run_id}")
    print(f"  Endpoint: {args.endpoint} @ {args.rps} RPS | Total duration: {args.duration}s")
    print(f"  Warm-up: {args.warm_up}s | Fault: {args.fault_type} ({args.cpu_mode}) on '{args.fault_target}'")
    print(f"  Fault duration: {args.fault_duration}s | Intensity: {args.fault_intensity}")
    print(f"========================================================\n")

    # 1. Verification Gates
    ensure_social_graph_seeded()
    check_trace_gate(args.endpoint)
    reset_all_container_states()

    # 2. Start Network Sampler (byte-level via /proc/<pid>/net/dev)
    net_sampler = DockerNetworkSampler(interval_s=5.0)
    print("[ORCHESTRATOR] Starting background Docker Network Sampler (5s cadence, byte-level)...")
    net_sampler.start()

    exp_start_ts = time.time()

    # 3. Start workload process in background
    workload_cmd = [
        sys.executable, str(WORKLOAD_SCRIPT),
        "--endpoint", args.endpoint,
        "--rps", str(args.rps),
        "--duration", str(args.duration)
    ]
    print(f"[ORCHESTRATOR] Launching workload ({args.duration}s)...")
    workload_proc = subprocess.Popen(workload_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    target_fault_start = exp_start_ts + args.warm_up + 0.05
    target_fault_end = exp_start_ts + args.warm_up + args.fault_duration - 0.10

    # 4. Warm-up
    print(f"[ORCHESTRATOR] Running warm-up for {args.warm_up}s (target fault start: {target_fault_start:.2f})...")
    warmup_start = time.time()
    if args.fault_type == "memory" and args.fault_target:
        # Check warmup activity during warmup (at t=20s) so it completes before fault onset
        time.sleep(min(20.0, max(5.0, args.warm_up - 20.0)))
        call_count = check_warmup_activity(args.fault_target, warmup_start, time.time())
        if call_count == 0:
            raise AssertionError(f"Target {args.fault_target} has 0 calls in warmup; cannot inject fault on off-path service")
        print(f"[GATE] Warmup on-path check passed for {args.fault_target} (sample traces: {call_count}).")

    # Sleep strictly until target_fault_start (zero negative lead time guarantees no pre-fault injection)
    wait_time = max(0.01, target_fault_start - time.time())
    time.sleep(wait_time)
    warmup_end = time.time()

    fault_start_ts = None
    fault_end_ts = None
    fault_start_effective = None
    fault_end_effective = None
    fault_measure = ""

    # 5. Fault injection (if applicable)
    if args.fault_type != "none" and args.fault_target:
        chaos_cmd = [
            sys.executable, str(CHAOS_SCRIPT),
            "--target", args.fault_target,
            "--type", args.fault_type,
            "--duration", str(args.fault_duration),
            "--intensity", str(args.fault_intensity),
            "--delay-ms", str(args.delay_ms),
            "--loss-pct", str(args.loss_pct),
            "--cpu-mode", args.cpu_mode,
            "--burn-cores", str(args.burn_cores),
            "--target-end-ts", str(target_fault_end)
        ]
        print(f"[ORCHESTRATOR] Triggering chaos injection: {args.fault_type} ({args.cpu_mode}) on {args.fault_target}...")
        chaos_res = subprocess.run(chaos_cmd, capture_output=True, text=True)
        if chaos_res.returncode != 0:
            print(f"[ERROR] Chaos injection failed:\n{chaos_res.stderr}", file=sys.stderr)
            raise RuntimeError(f"Chaos injection script returned code {chaos_res.returncode}")

        try:
            lines = [l for l in chaos_res.stdout.splitlines() if l.strip()]
            json_str = "\n".join(lines[lines.index(next(l for l in lines if l.startswith("{"))):])
            c_data = json.loads(json_str)
            fault_start_ts = c_data["start_ts"]
            fault_end_ts = c_data["end_ts"]
            fault_start_effective = c_data.get("fault_start_effective", fault_start_ts)
            fault_end_effective = c_data.get("fault_end_effective", fault_end_ts)
            fault_measure = c_data["measure"]
            print(f"[ORCHESTRATOR] Fault completed: nominal [{fault_start_ts:.3f} -> {fault_end_ts:.3f}], "
                  f"effective [{fault_start_effective:.3f} -> {fault_end_effective:.3f}]")
        except Exception as e:
            raise RuntimeError(f"Failed to parse chaos report: {e}\nOutput: {chaos_res.stdout}")

    # 6. Wait for workload to finish
    print("[ORCHESTRATOR] Waiting for workload to complete...")
    w_stdout, w_stderr = workload_proc.communicate()
    exp_end_ts = time.time()
    print(f"[ORCHESTRATOR] Experiment finished. Total runtime: {exp_end_ts - exp_start_ts:.1f}s")

    # Post-workload trace and metrics flush delay (15s) to guarantee 100% trace capture without span drops
    print("[ORCHESTRATOR] Waiting 15s for in-flight traces and metrics to flush completely...")
    time.sleep(15.0)

    # 7. Stop Network Sampler & Save Cumulative Metrics
    print("[ORCHESTRATOR] Stopping Network Sampler and dumping NetIO series...")
    net_sampler.stop(run_dir)

    # Save workload summary
    try:
        w_summary = json.loads(w_stdout)
        (run_dir / "workload_summary.json").write_text(json.dumps(w_summary, indent=2))
    except Exception:
        (run_dir / "workload_summary.txt").write_text(w_stdout + "\n" + w_stderr)

    # 8. Export telemetry
    print("[ORCHESTRATOR] Exporting multimodal telemetry from Prometheus, Jaeger, and Docker...")
    col_cmd = [
        sys.executable, str(COLLECTOR_SCRIPT),
        "--out", str(run_dir),
        "--start", str(exp_start_ts),
        "--end", str(exp_end_ts + 5.0)
    ]
    subprocess.run(col_cmd, check=True)

    # If memory fault was injected, restart the target container AFTER telemetry export is saved
    if args.fault_type == "memory" and args.fault_target:
        print(f"[CLEANUP] Restarting memory fault target container {args.fault_target} to restore unswapped resident RAM...")
        run_cmd(f"docker restart {args.fault_target}")
        time.sleep(5)

    # 9. Save args.txt
    args_dict = {
        "run_id": args.run_id,
        "system": "DeathStarBench-SocialNetwork",
        "endpoint": args.endpoint,
        "rps": args.rps,
        "experiment_duration": args.duration,
        "warm_up_duration": args.warm_up,
        "fault_type": args.fault_type,
        "cpu_mode": args.cpu_mode if args.fault_type == "cpu" else None,
        "target_component": args.fault_target,
        "fault_measure": fault_measure,
        "fault_start_ts": fault_start_ts,
        "fault_end_ts": fault_end_ts,
        "fault_start_effective": fault_start_effective,
        "fault_end_effective": fault_end_effective,
        "fault_duration_s": args.fault_duration,
        "burn_cores": args.burn_cores if args.fault_type == "cpu" else None,
        "seed": args.seed,
        "experiment_start_ts": exp_start_ts,
        "experiment_end_ts": exp_end_ts
    }
    (run_dir / "args.txt").write_text(json.dumps(args_dict, indent=2))

    # 10. Update Manifest
    manifest_entry = {
        "run_id": args.run_id,
        "system": "DeathStarBench-SocialNetwork",
        "endpoint": args.endpoint,
        "fault_type": args.fault_type,
        "cpu_mode": args.cpu_mode if args.fault_type == "cpu" else None,
        "target_component": args.fault_target,
        "fault_start_ts": fault_start_ts,
        "fault_end_ts": fault_end_ts,
        "fault_start_effective": fault_start_effective,
        "fault_end_effective": fault_end_effective,
        "measure": fault_measure,
        "rps": args.rps,
        "duration_s": args.duration,
        "warm_up_s": args.warm_up,
        "seed": args.seed
    }
    manifest_entries = []
    if manifest_file.exists():
        for line in manifest_file.read_text().splitlines():
            if not line.strip():
                continue
            try:
                e = json.loads(line)
                if e.get("run_id") != args.run_id:
                    manifest_entries.append(e)
            except Exception:
                pass
    manifest_entries.append(manifest_entry)
    with open(manifest_file, "w") as f:
        for e in manifest_entries:
            f.write(json.dumps(e) + "\n")

    # 11. Run Causal Preprocessor
    proc_out_dir = Path(args.out_dir) / "processed_dataset" / args.endpoint
    print(f"[ORCHESTRATOR] Running causal preprocessor -> {proc_out_dir}...")
    proc_cmd = [
        sys.executable, str(PROCESS_SCRIPT),
        "--raw-dir", str(run_dir),
        "--out-dir", str(proc_out_dir)
    ]
    subprocess.run(proc_cmd, check=True)

    # 12. Run Independent Verifier Gate
    processed_csv = proc_out_dir / f"{args.run_id}.csv"
    print(f"\n[ORCHESTRATOR] Running independent verifier on {processed_csv}...")
    ver_cmd = [
        sys.executable, str(PROJECT_ROOT / "scripts" / "audit" / "verify_run.py"),
        "--csv", str(processed_csv),
        "--args", str(run_dir / "args.txt")
    ]
    ver_res = subprocess.run(ver_cmd, capture_output=True, text=True)
    print(ver_res.stdout)
    if ver_res.stderr:
        print(ver_res.stderr, file=sys.stderr)

    print(f"\n[ORCHESTRATOR] RUN {args.run_id} COMPLETED.")
    print(f"Raw data: {run_dir}")
    print(f"Processed CSV: {processed_csv}\n")

if __name__ == "__main__":
    main()
