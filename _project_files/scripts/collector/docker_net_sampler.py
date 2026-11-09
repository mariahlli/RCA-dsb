#!/usr/bin/env python3
"""
scripts/collector/docker_net_sampler.py
Background cumulative network sampler for Docker containers.
Queries Docker Engine API directly over the Unix socket (/var/run/docker.sock)
using stats?stream=false&one-shot=true for exact byte-level rx_bytes / tx_bytes
precision (takes ~0.15s for all 26 containers).

Usage:
  Imported by run_experiment.py and run as a background thread.
  - start(): begins sampling every interval_s
  - stop(out_dir): stops sampling, writes Prometheus-formatted JSON series
"""
import sys
import time
import json
import socket
import http.client
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path="/var/run/docker.sock"):
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)

def query_container_net(name):
    try:
        conn = UnixHTTPConnection()
        conn.request("GET", f"/containers/{name}/stats?stream=false&one-shot=true")
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        conn.close()
        networks = data.get("networks", {})
        rx = sum(net.get("rx_bytes", 0) for net in networks.values())
        tx = sum(net.get("tx_bytes", 0) for net in networks.values())
        return name, rx, tx
    except Exception as e:
        return name, None, None

class DockerNetworkSampler:
    def __init__(self, interval_s=5.0):
        self.interval_s = interval_s
        self.stop_event = threading.Event()
        self.thread = None
        self.rx_series = {}   # svc_name -> list of [ts, byte_count_str]
        self.tx_series = {}   # svc_name -> list of [ts, byte_count_str]
        self.container_names = []

    def _discover_containers(self):
        try:
            conn = UnixHTTPConnection()
            conn.request("GET", "/containers/json")
            resp = conn.getresponse()
            containers = json.loads(resp.read().decode("utf-8"))
            conn.close()
            names = []
            for c in containers:
                for name in c.get("Names", []):
                    clean = name.lstrip("/")
                    if "socialnetwork-" in clean or "social-network-" in clean:
                        names.append(clean)
            self.container_names = names
            print(f"[NET_SAMPLER] Discovered {len(self.container_names)} containers via Docker API")
        except Exception as e:
            print(f"[NET_SAMPLER WARN] Container discovery failed: {e}", file=sys.stderr)

    def _sample_once(self):
        if not self.container_names:
            self._discover_containers()
            if not self.container_names:
                return

        ts = time.time()
        with ThreadPoolExecutor(max_workers=min(20, len(self.container_names))) as pool:
            results = list(pool.map(query_container_net, self.container_names))

        for cname, rx, tx in results:
            if rx is None or tx is None:
                continue

            clean = cname.replace("socialnetwork-", "").replace("social-network-", "")
            if clean.endswith("-1"):
                clean = clean[:-2]

            if clean not in self.rx_series:
                self.rx_series[clean] = []
                self.tx_series[clean] = []

            self.rx_series[clean].append([round(ts, 3), str(rx)])
            self.tx_series[clean].append([round(ts, 3), str(tx)])

    def _run_loop(self):
        while not self.stop_event.is_set():
            t_start = time.time()
            self._sample_once()
            elapsed = time.time() - t_start
            sleep_time = max(0.1, self.interval_s - elapsed)
            self.stop_event.wait(sleep_time)

    def start(self):
        self.stop_event.clear()
        self._discover_containers()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self, out_dir: Path):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
        # Final sample
        self._sample_once()

        prom_dir = out_dir / "prom_metrics"
        prom_dir.mkdir(parents=True, exist_ok=True)
        for svc, values in self.rx_series.items():
            out_file = prom_dir / f"{svc}_container_network_receive_bytes_total"
            out_file.write_text(json.dumps(values, indent=2))
        for svc, values in self.tx_series.items():
            out_file = prom_dir / f"{svc}_container_network_transmit_bytes_total"
            out_file.write_text(json.dumps(values, indent=2))
        print(f"[NET_SAMPLER] Saved byte-level network series for {len(self.rx_series)} containers to {prom_dir}")
