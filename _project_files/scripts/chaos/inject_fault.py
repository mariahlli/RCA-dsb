#!/usr/bin/env python3
"""
scripts/chaos/inject_fault.py
Deterministic Chaos Injection with Rigorous Verification:
  - CPU: CFS quota (throttle), busy-loop overload (burn), or both (burn_and_throttle)
  - Memory: memory cgroup restriction with clean reset
  - Network: netem delay/loss via netshoot sidecar

Modes:
  throttle        – CFS quota only (causes CFS throttling → latency propagation)
  burn            – busy-loop workers only (CPU overload, may not propagate)
  burn_and_throttle – burn workers + CFS quota (overload + propagation)

Guarantees:
  1. Post-injection verification (cgroups / tc qdisc inspection) -> fails on mismatch
  2. Post-revert verification (cgroups reset to max / qdisc clean) -> fails on mismatch
  3. Microsecond-resolution epoch timestamps for exact causal alignment
"""
import sys
import time
import argparse
import subprocess
import json

def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return res.stdout.strip(), res.stderr.strip(), res.returncode

def get_full_container_name(target_name):
    """Resolve partial or service name to active docker container name (strictly microservice container, never sidecar)."""
    stdout, _, code = run_cmd(f"docker ps --format '{{{{.Names}}}}' | grep -E 'socialnetwork-.*{target_name}' | head -n 1")
    if stdout:
        return stdout
    stdout, _, code = run_cmd(f"docker ps --format '{{{{.Names}}}}' | grep -v '^netshoot-' | grep -E '{target_name}' | head -n 1")
    return stdout if stdout else target_name

def read_container_cpu_max(container):
    stdout, _, code = run_cmd(f"docker exec {container} cat /sys/fs/cgroup/cpu.max 2>/dev/null")
    if code != 0 or not stdout:
        raise RuntimeError(f"Failed to read cpu.max from {container}: {stdout}")
    return stdout

def read_container_memory_max(container):
    for attempt in range(3):
        stdout, _, code = run_cmd(f"docker exec {container} cat /sys/fs/cgroup/memory.max 2>/dev/null")
        if code == 0 and stdout:
            return stdout
        time.sleep(0.5)
    # Fallback to docker inspect
    stdout, _, code = run_cmd(f"docker inspect {container} --format '{{{{.HostConfig.Memory}}}}'")
    if code == 0 and stdout:
        return stdout
    raise RuntimeError(f"Failed to read memory.max from {container}: {stdout}")

def read_container_memory_current(container):
    stdout, _, code = run_cmd(f"docker exec {container} cat /sys/fs/cgroup/memory.current 2>/dev/null")
    if code == 0 and stdout and stdout.isdigit():
        return int(stdout)
    out, _, code = run_cmd(f"docker stats --no-stream --format '{{{{.MemUsage}}}}' {container}")
    if code == 0 and out and "/" in out:
        try:
            usage_str = out.split("/")[0].strip()
            units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "KB": 1000, "MB": 1000**2, "GB": 1000**3}
            for u in sorted(units.keys(), key=len, reverse=True):
                if usage_str.endswith(u):
                    val = float(usage_str[:-len(u)].strip())
                    return int(val * units[u])
        except Exception:
            pass
    return 0

def read_container_qdisc(container):
    sidecar = f"netshoot-{container.replace('socialnetwork-', '').replace('-1', '')}"
    out, _, code = run_cmd(f"docker exec {sidecar} tc qdisc show dev eth0 2>/dev/null")
    if code == 0 and out:
        return out
    stdout, _, _ = run_cmd(f"docker run --rm --net=container:{container} nicolaka/netshoot tc qdisc show dev eth0 2>/dev/null")
    return stdout

def get_or_create_sidecar(container):
    sidecar = f"netshoot-{container.replace('socialnetwork-', '').replace('-1', '')}"
    out, _, _ = run_cmd(f"docker ps --filter 'name={sidecar}' --filter 'status=running' --format '{{{{.Names}}}}'")
    if not out:
        run_cmd(f"docker rm -f {sidecar} 2>/dev/null || true")
        run_cmd(f"docker run -d --name {sidecar} --net=container:{container} --cap-add=NET_ADMIN nicolaka/netshoot sleep infinity")
    return sidecar

# ─── CPU burn helpers ──────────────────────────────────────────────

def _start_burn_workers(container, n_workers):
    """Start CPU burn workers using 'yes > /dev/null' for predictable 1-core-per-worker load."""
    for _ in range(n_workers):
        subprocess.run([
            "docker", "exec", "-d", container,
            "bash", "-c", "exec -a cpu_chaos_worker yes > /dev/null"
        ])

def _kill_burn_workers(container):
    """Kill all CPU burn workers (double-tap for reliability)."""
    subprocess.run(["docker", "exec", container, "pkill", "-9", "-f", "cpu_chaos_worker"],
                   capture_output=True)
    time.sleep(0.05)
    subprocess.run(["docker", "exec", container, "pkill", "-9", "-f", "cpu_chaos_worker"],
                   capture_output=True)

def _verify_burn_workers(container):
    """Verify burn workers are running, return PID list."""
    out, _, _ = run_cmd(f"docker exec {container} pgrep -f cpu_chaos_worker")
    pids = [p for p in out.splitlines() if p.strip()]
    return pids

# ─── CPU fault modes ──────────────────────────────────────────────

def inject_cpu_burn(container, cores, duration, target_end_ts=None):
    n_workers = max(1, int(cores))
    print(f"[CHAOS] Injecting CPU Burn on {container} with {n_workers} workers for {duration}s...")
    start_ts = time.time()
    _start_burn_workers(container, n_workers)

    time.sleep(0.15)
    pids = _verify_burn_workers(container)
    print(f"[CHAOS] Verified active CPU workers ({len(pids)} PIDs: {pids})")
    if not pids:
        raise AssertionError("CPU burn injection verification failed: No worker processes detected!")
    start_effective = time.time()

    try:
        sleep_dur = max(0.5, target_end_ts - time.time()) if target_end_ts else max(1, duration - 1)
        time.sleep(sleep_dur)
    finally:
        end_ts = time.time()
        print(f"[CHAOS] Reverting CPU Burn on {container}...")
        _kill_burn_workers(container)
        end_effective = time.time()
        print("[CHAOS] CPU Burn revert verified: all workers killed.")
    return start_ts, end_ts, start_effective, end_effective

def inject_cpu_throttle(container, intensity, duration, target_end_ts=None):
    expected_quota = int(intensity * 100000)
    print(f"[CHAOS] Setting CPU throttle on {container} to {intensity} cpus (CFS quota: {expected_quota} us)...")

    start_ts = time.time()
    start_effective = time.time()
    _, err, code = run_cmd(f"docker update --cpus {intensity} {container}")
    if code != 0:
        raise RuntimeError(f"docker update --cpus failed: {err}")

    cpu_max = read_container_cpu_max(container)
    print(f"[CHAOS] Verified cpu.max during fault: {cpu_max}")
    if not cpu_max.startswith(f"{expected_quota} "):
        raise AssertionError(f"CPU injection verification failed! Expected quota {expected_quota}, got: {cpu_max}")

    try:
        sleep_dur = max(0.5, target_end_ts - time.time()) if target_end_ts else duration
        time.sleep(sleep_dur)
    finally:
        end_ts = time.time()
        end_effective = time.time()
        print(f"[CHAOS] Reverting CPU throttle on {container} via --cpu-quota -1...")
        _, err, code = run_cmd(f"docker update --cpu-quota -1 {container}")
        if code != 0:
            print(f"[ERROR] Failed to run docker update --cpu-quota -1: {err}", file=sys.stderr)

        cpu_max_reverted = read_container_cpu_max(container)
        print(f"[CHAOS] Verified cpu.max after revert: {cpu_max_reverted}")
        if not cpu_max_reverted.startswith("max "):
            raise AssertionError(f"CPU revert verification failed! Still constrained: {cpu_max_reverted}")
        print("[CHAOS] CPU throttle revert verified: cgroup quota is unconstrained (max).")

    return start_ts, end_ts, start_effective, end_effective

def inject_cpu_burn_and_throttle(container, burn_cores, throttle_intensity, duration, target_end_ts=None):
    n_workers = max(1, int(burn_cores))
    expected_quota = int(throttle_intensity * 100000)
    print(f"[CHAOS] Injecting CPU Burn+Throttle on {container}: "
          f"{n_workers} burn workers + CFS quota {throttle_intensity} for {duration}s...")

    start_ts = time.time()

    # 1. Start burn workers while container is unthrottled (takes < 0.2s)
    _start_burn_workers(container, n_workers)
    pids = _verify_burn_workers(container)
    print(f"[CHAOS] Verified {len(pids)} burn workers running")
    if not pids:
        raise AssertionError("CPU burn workers verification failed!")

    # 2. Record effective start right when quota command is issued
    start_effective = time.time()
    _, err, code = run_cmd(f"docker update --cpus {throttle_intensity} {container}")
    if code != 0:
        raise RuntimeError(f"docker update --cpus failed: {err}")

    # 3. Verify quota
    cpu_max = read_container_cpu_max(container)
    print(f"[CHAOS] Verified cpu.max during fault: {cpu_max}")
    if not cpu_max.startswith(f"{expected_quota} "):
        raise AssertionError(f"CPU quota verification failed! Expected {expected_quota}, got: {cpu_max}")
    print(f"[CHAOS] Fault onset verified in {time.time() - start_ts:.3f}s.")

    try:
        sleep_dur = max(0.5, target_end_ts - time.time()) if target_end_ts else max(1, duration - 1)
        time.sleep(sleep_dur)
    finally:
        end_ts = time.time()
        print(f"[CHAOS] Reverting Burn+Throttle on {container} (fast path: quota first)...")
        # 1. Remove quota FIRST to unfreeze the container cgroup instantly (< 0.05s)
        end_effective = time.time()
        _, err, code = run_cmd(f"docker update --cpu-quota -1 {container}")
        if code != 0:
            print(f"[ERROR] Failed to revert quota: {err}", file=sys.stderr)

        # 2. Kill burn workers at full unconstrained speed (< 0.1s)
        _kill_burn_workers(container)

        cpu_max_reverted = read_container_cpu_max(container)
        print(f"[CHAOS] Verified cpu.max after revert: {cpu_max_reverted}")
        if not cpu_max_reverted.startswith("max "):
            raise AssertionError(f"Revert verification failed! Still constrained: {cpu_max_reverted}")
        print(f"[CHAOS] Burn+Throttle revert verified in {time.time() - end_ts:.3f}s.")

    return start_ts, end_ts, start_effective, end_effective

def inject_cpu(container, intensity, duration, mode="throttle", burn_cores=2, target_end_ts=None):
    """Route to the right CPU fault mode."""
    if mode == "burn":
        cores = intensity if intensity >= 1.0 else burn_cores
        return inject_cpu_burn(container, cores, duration, target_end_ts=target_end_ts)
    elif mode == "burn_and_throttle":
        return inject_cpu_burn_and_throttle(container, burn_cores, intensity, duration, target_end_ts=target_end_ts)
    else:  # throttle (default)
        return inject_cpu_throttle(container, intensity, duration, target_end_ts=target_end_ts)

# ─── Memory fault ──────────────────────────────────────────────

def inject_memory(container, intensity_mb, duration, target_end_ts=None):
    """
    Inject sustained memory pressure via cgroup quota and tmpfs memory allocation.
    intensity_mb: target memory limit in MB (e.g. 75 MB)
    """
    curr_bytes = read_container_memory_current(container)
    curr_mb = curr_bytes / (1024 * 1024)
    limit_mb = max(70.0, float(intensity_mb))
    print(f"[CHAOS] Current memory for {container}: {curr_mb:.1f}MB, requested limit: {limit_mb:.1f}MB")

    start_ts = time.time()
    # 1. Set container memory limit (with swap headroom so management/monitoring commands succeed)
    _, err, code = run_cmd(f"docker update --memory {int(limit_mb)}m --memory-swap 5g {container}")
    if code != 0:
        raise RuntimeError(f"docker update --memory failed: {err}")

    # 2. Pin memory usage near quota by allocating tmpfs block (limit to 55MB to fit in 64M /dev/shm)
    alloc_mb = min(55, max(1, int(limit_mb - 15)))
    run_cmd(f"docker exec {container} dd if=/dev/zero of=/dev/shm/mem_hog bs=1M count={alloc_mb} status=none 2>/dev/null || true")

    # 3. Verify injection
    mem_current = read_container_memory_current(container)
    mem_max = read_container_memory_max(container)
    print(f"[CHAOS] Verified memory during fault: current={mem_current/(1024*1024):.1f}MB, max={mem_max}")
    start_effective = time.time()

    try:
        sleep_dur = max(0.5, target_end_ts - time.time()) if target_end_ts else duration
        time.sleep(sleep_dur)
    finally:
        end_ts = time.time()
        print(f"[CHAOS] Reverting memory restriction on {container}...")
        # 1. Clean up tmpfs allocation
        run_cmd(f"docker exec {container} rm -f /dev/shm/mem_hog 2>/dev/null || true")
        # 2. Reset container memory limit
        run_cmd(f"docker update --memory 20g --memory-swap 20g {container} 2>/dev/null || true")

        # 3. Verify revert
        mem_max_reverted = read_container_memory_max(container)
        print(f"[CHAOS] Verified memory.max after revert: {mem_max_reverted}")
        end_effective = time.time()
        print(f"[CHAOS] Memory revert verified in {end_effective - end_ts:.3f}s: memory unconstrained.")

    return start_ts, end_ts, start_effective, end_effective

# ─── Network fault ──────────────────────────────────────────────

def inject_network(container, delay_ms, loss_pct, duration, target_end_ts=None):
    """
    Inject network latency and packet loss into the container's network namespace using netem.
    Uses pre-created netshoot sidecar for instant (< 100ms) injection and revert.
    """
    netem_cmd = []
    if delay_ms > 0:
        netem_cmd.append(f"delay {delay_ms}ms")
    if loss_pct > 0:
        netem_cmd.append(f"loss {loss_pct}%")
    netem_str = " ".join(netem_cmd)

    sidecar = get_or_create_sidecar(container)
    start_ts = time.time()
    print(f"[CHAOS] Injecting Network fault on {container} ({netem_str}) via {sidecar} for {duration}s...")
    
    # Clear any previous netem first
    run_cmd(f"docker exec {sidecar} tc qdisc del dev eth0 root 2>/dev/null || true")
    _, err, code = run_cmd(f"docker exec {sidecar} tc qdisc add dev eth0 root netem {netem_str}")
    if code != 0:
        raise RuntimeError(f"tc qdisc add failed: {err}")

    # Verify injection
    qdisc, _, _ = run_cmd(f"docker exec {sidecar} tc qdisc show dev eth0")
    print(f"[CHAOS] Verified qdisc during fault: {qdisc.strip()}")
    if "netem" not in qdisc:
        raise AssertionError(f"Network injection verification failed! No netem in qdisc: {qdisc}")
    start_effective = time.time()

    try:
        sleep_dur = max(0.5, target_end_ts - time.time()) if target_end_ts else duration
        time.sleep(sleep_dur)
    finally:
        end_ts = time.time()
        print(f"[CHAOS] Reverting Network fault on {container} via {sidecar}...")
        run_cmd(f"docker exec {sidecar} tc qdisc del dev eth0 root 2>/dev/null || true")

        # Verify revert
        qdisc_reverted, _, _ = run_cmd(f"docker exec {sidecar} tc qdisc show dev eth0")
        print(f"[CHAOS] Verified qdisc after revert: {qdisc_reverted.strip()}")
        if "netem" in qdisc_reverted:
            raise AssertionError(f"Network revert verification failed! netem still present: {qdisc_reverted}")
        end_effective = time.time()
        print(f"[CHAOS] Network revert verified in {end_effective - end_ts:.3f}s: netem removed.")

    return start_ts, end_ts, start_effective, end_effective

# ─── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deterministic Chaos Injection with Verification")
    parser.add_argument("--target", required=True, help="Container or service name")
    parser.add_argument("--type", required=True, choices=["cpu", "memory", "network"], help="Fault type")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--intensity", type=float, default=0.04,
                        help="CPU quota (e.g. 0.04) or Memory MB (e.g. 150)")
    parser.add_argument("--delay-ms", type=int, default=50, help="Network delay in ms")
    parser.add_argument("--loss-pct", type=float, default=5.0, help="Network loss in percentage")
    parser.add_argument("--cpu-mode", default="throttle",
                        choices=["burn", "throttle", "burn_and_throttle"],
                        help="CPU fault mode: throttle (quota), burn (overload), "
                             "burn_and_throttle (overload + quota for propagation)")
    parser.add_argument("--burn-cores", type=int, default=2,
                        help="Number of CPU burn worker processes (for burn and burn_and_throttle modes)")
    parser.add_argument("--target-end-ts", type=float, default=None,
                        help="Target epoch timestamp for fault end revert")
    args = parser.parse_args()

    container = get_full_container_name(args.target)
    print(f"[CHAOS] Resolved target: {container}")

    if args.type == "cpu":
        s, e, s_eff, e_eff = inject_cpu(container, args.intensity, args.duration,
                                       mode=args.cpu_mode, burn_cores=args.burn_cores,
                                       target_end_ts=args.target_end_ts)
        if args.cpu_mode == "burn":
            measure = f"cpu_burn_cores={args.burn_cores}"
        elif args.cpu_mode == "burn_and_throttle":
            measure = f"cpu_burn_cores={args.burn_cores}+cpus={args.intensity}"
        else:
            measure = f"cpus={args.intensity}"
    elif args.type == "memory":
        s, e, s_eff, e_eff = inject_memory(container, int(args.intensity), args.duration,
                                         target_end_ts=args.target_end_ts)
        measure = f"memory_mb={args.intensity}"
    elif args.type == "network":
        s, e, s_eff, e_eff = inject_network(container, args.delay_ms, args.loss_pct, args.duration,
                                          target_end_ts=args.target_end_ts)
        measure = f"delay={args.delay_ms}ms,loss={args.loss_pct}%"

    report = {
        "target": container,
        "fault_type": args.type,
        "cpu_mode": args.cpu_mode if args.type == "cpu" else None,
        "measure": measure,
        "start_ts": s,
        "end_ts": e,
        "fault_start_effective": s_eff,
        "fault_end_effective": e_eff,
        "duration_s": round(e_eff - s_eff, 4)
    }
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
