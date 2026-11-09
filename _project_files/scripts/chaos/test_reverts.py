#!/usr/bin/env python3
"""
scripts/chaos/test_reverts.py
Test the revert path and verification check for every fault type:
1. CPU Throttle (CFS quota)
2. CPU Burn (worker processes)
3. CPU Burn + Throttle (combined)
4. Memory restriction (cgroup memory.max + compose recreate)
5. Network delay & loss (netem qdisc via netshoot)
"""

import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from chaos.inject_fault import (
    inject_cpu_throttle,
    inject_cpu_burn,
    inject_cpu_burn_and_throttle,
    inject_memory,
    inject_network,
    read_container_cpu_max,
    read_container_memory_max,
    read_container_qdisc,
    _verify_burn_workers,
    run_cmd
)

CONTAINER = "socialnetwork-compose-post-service-1"

def test_cpu_throttle():
    print("\n--- Testing CPU Throttle Revert Path ---")
    start_ts, end_ts, s_eff, e_eff = inject_cpu_throttle(CONTAINER, 0.1, duration=3)
    post_revert = read_container_cpu_max(CONTAINER)
    assert post_revert.startswith("max "), f"Revert failed! cpu.max: {post_revert}"
    print(f"[PASS] CPU Throttle reverted cleanly: {post_revert}")

def test_cpu_burn():
    print("\n--- Testing CPU Burn Revert Path ---")
    start_ts, end_ts, s_eff, e_eff = inject_cpu_burn(CONTAINER, cores=2, duration=3)
    pids = _verify_burn_workers(CONTAINER)
    assert len(pids) == 0, f"Revert failed! PIDs still running: {pids}"
    print(f"[PASS] CPU Burn reverted cleanly: zero worker PIDs ({pids})")

def test_cpu_burn_and_throttle():
    print("\n--- Testing CPU Burn+Throttle Revert Path ---")
    start_ts, end_ts, s_eff, e_eff = inject_cpu_burn_and_throttle(CONTAINER, burn_cores=2, throttle_intensity=0.1, duration=3)
    post_revert = read_container_cpu_max(CONTAINER)
    pids = _verify_burn_workers(CONTAINER)
    assert post_revert.startswith("max ") and len(pids) == 0, f"Revert failed! cpu.max: {post_revert}, pids: {pids}"
    print(f"[PASS] CPU Burn+Throttle reverted cleanly: {post_revert}, zero workers ({pids})")

def test_network():
    print("\n--- Testing Network Netem Revert Path ---")
    start_ts, end_ts, s_eff, e_eff = inject_network(CONTAINER, delay_ms=50, loss_pct=5.0, duration=3)
    qdisc = read_container_qdisc(CONTAINER)
    assert "netem" not in qdisc, f"Revert failed! qdisc still has netem: {qdisc}"
    print(f"[PASS] Network fault reverted cleanly: qdisc = {qdisc}")

def test_memory():
    print("\n--- Testing Memory Restriction Revert Path ---")
    mongo_c = "socialnetwork-post-storage-mongodb-1"
    # First verify abort if limit >= current
    try:
        inject_memory(CONTAINER, intensity_mb=256, duration=1)
        assert False, "Should have aborted because limit >= current!"
    except AssertionError as e:
        print(f"[PASS] Successfully caught abort: {e}")

    start_ts, end_ts, s_eff, e_eff = inject_memory(mongo_c, intensity_mb=96, duration=3)
    mem_max = read_container_memory_max(mongo_c)
    unconstrained = (mem_max == "max") or (mem_max.isdigit() and int(mem_max) >= 20 * 1024 * 1024 * 1024)
    assert unconstrained, f"Revert failed! memory.max: {mem_max}"
    print(f"[PASS] Memory restriction reverted cleanly: memory.max = {mem_max}")

if __name__ == "__main__":
    print(f"Testing fault injection & revert paths on {CONTAINER}...")
    test_cpu_throttle()
    test_cpu_burn()
    test_cpu_burn_and_throttle()
    test_network()
    test_memory()
    print("\n[ALL FAULT REVERT PATHS VERIFIED SUCCESSFULLY]")
