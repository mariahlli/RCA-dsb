# DeathStarBench RCA Data Collection

This project collects multimodal telemetry from the **DeathStarBench Social Network** microservice benchmark to support root-cause analysis (RCA) research. It orchestrates workload generation and controlled CPU, memory, or network faults; collects Prometheus metrics, Jaeger traces, Docker logs, and network counters; preprocesses the run into time-windowed CSV data; and verifies the resulting dataset.

The current codebase is a **data-collection and preprocessing pipeline**. It does not contain the model-training, explainability, or evaluation implementation described in the old README.

## What is in this project

This README is in `_project_files/`, which contains the scripts and configs:

```text
_project_files/
├── config/                 # Jaeger configuration
├── configs/                # Campaign and active-column schemas
├── deploy/                 # Prometheus/cAdvisor/Jaeger Compose overlay
├── scripts/
│   ├── audit/              # Dataset and per-run verification
│   ├── chaos/              # Fault injection and optional network sidecars
│   ├── collector/          # Prometheus, Jaeger, Docker, and network collection
│   ├── orchestration/      # Single-run and pilot campaign runners
│   ├── preprocessing/      # Raw-run to processed-CSV conversion
│   └── workload/           # wrk2 workload launcher
└── requirements.txt        # Pinned scientific/modeling Python packages
```

In the surrounding workspace, `results/new_dataset/` contains collected raw and processed data, and `docs/tache3_deathstarbench_benchmark.md` describes the benchmark design. A run launched from a project copy writes to that copy's `results/new_dataset/` by default.

## Requirements

- **WSL2/Linux is the intended runtime.** The scripts use Bash, Docker host networking, Linux cgroups and `tc`/netem, and the Docker Unix socket. Running them directly in native Windows Python is not supported.
- Docker Engine with the Compose v2 plugin, permission to use Docker without interactive prompts, and permission to use the Docker socket and network-administration capabilities.
- A working DeathStarBench Social Network checkout, including its `docker-compose.yml`, database initialization script, and wrk2 Lua workload scripts.
- The `rca-wrk2:latest` Docker image. `run_workload.py` invokes this image; this repository does not include its Dockerfile or build recipe.
- Python on the WSL/Linux host. The benchmark initialization container uses Python 3.10. For installing the complete pinned `requirements.txt`, use a Python version supported by its pinned PyTorch 2.6.0 package (for example, Python 3.10–3.12); **Python 3.14 is not a supported target for that pin**.
- Available host ports: `8080` (benchmark HTTP), `9090` (Prometheus), `8088` (cAdvisor), `16686` (Jaeger UI/API), and Jaeger collector/agent ports `14268`, `14269`, `6831/udp`, and `6832/udp`.

The collection, preprocessing, and verification scripts primarily use the Python standard library. `requirements.txt` also installs heavy scientific and deep-learning packages (including PyTorch); install it only if your work needs those packages and the project's approval process allows it. The file itself notes that approval is required. Its CUDA comment refers to a CUDA 12.4 PyTorch wheel, not a CUDA 13 wheel.

## Configure paths before launching

Several scripts currently contain fixed paths from the original WSL machine. The documented launch commands work as written only if the checkout and benchmark are at these locations:

```text
Project:   /home/maria/rca-project
Benchmark: /home/maria/DeathStarBench/socialNetwork
wrk2 Lua:  /home/maria/DeathStarBench/socialNetwork/wrk2/scripts/social-network
```

If your WSL paths differ, update the corresponding constants before running:

- `deploy/up.sh` and `deploy/down.sh`: `BENCH_DIR` and `DEPLOY_DIR`.
- `deploy/docker-compose-telemetry.yml`: the host path mounted for `prometheus.yml`.
- `scripts/workload/run_workload.py`: `LUA_DIR`.
- `scripts/orchestration/run_experiment.py`: benchmark path used when initializing the social graph.

`PROJECT_ROOT` can be set to locate project scripts and output data, but it does **not** override those other hard-coded benchmark and deployment paths.

The included `scripts/sync_to_wsl.sh` and `scripts/sync_to_win.sh` are machine-specific helpers, not reliable project-wide synchronization commands. In particular, `sync_to_wsl.sh` references the absent `scripts/audit/run_task7_pilots.py` and does not copy all files required by the runner (including the workload script). Prepare a complete working copy in WSL and verify the paths above rather than relying on these helpers as-is.

## Launch the benchmark and telemetry stack

Run these commands from the **WSL/Linux project root**:

```bash
cd /home/maria/rca-project
bash deploy/up.sh
```

The script starts the DeathStarBench Compose stack with the telemetry overlay and initializes the social graph. It expects the DeathStarBench checkout and telemetry files at the configured paths. Check the main endpoints before collecting:

```bash
curl -fsS http://localhost:8080/ >/dev/null && echo "Benchmark HTTP is reachable"
curl -fsS http://localhost:9090/-/ready
curl -fsS http://localhost:16686/ >/dev/null && echo "Jaeger is reachable"
```

If using network faults, create the optional netshoot sidecars after the benchmark containers are up:

```bash
bash scripts/chaos/setup_sidecars.sh
```

The fault injector can also create a sidecar for its target when needed. Do not start a campaign until the benchmark, Prometheus, Jaeger, and Docker API are reachable.

## Run data collection

First run one of the defined pilots. For example, this runs the configured 120-second high-load baseline:

```bash
python3 scripts/orchestration/run_pilots.py \
  --run-id pilot_baseline_compose_200rps_120s
```

The pilot runner invokes the end-to-end orchestrator and then `verify_run.py`. It stops when a run or its verification fails. `--limit N` runs only the first N configured pilots; omitting both options runs the complete pilot list sequentially:

```bash
python3 scripts/orchestration/run_pilots.py --limit 1
# Full configured pilot list:
python3 scripts/orchestration/run_pilots.py
```

Each pilot may take several minutes; the full list takes substantially longer. Existing pilot outputs that pass verification are skipped. Use unique run IDs for experiments outside the predefined pilot list.

To configure a single experiment directly, use `run_experiment.py`:

```bash
python3 scripts/orchestration/run_experiment.py \
  --run-id example_cpu_throttle_compose_50rps \
  --endpoint compose \
  --rps 50 \
  --duration 180 \
  --warm-up 60 \
  --fault-type cpu \
  --cpu-mode throttle \
  --fault-target compose-post-service \
  --fault-duration 30 \
  --fault-intensity 0.04
```

Supported endpoints are `compose`, `home`, `user`, and `mixed`. Supported fault types are `none`, `cpu`, `memory`, and `network`; consult `--help` for the other controls. Fault injection changes live container resource limits or network behavior, so use a disposable benchmark environment and do not target unrelated containers.

The orchestrator automatically collects telemetry and runs preprocessing. By default, output is written beneath `results/new_dataset/` under `PROJECT_ROOT` (default: `~/rca-project`). You can override the project root and output root when appropriate:

```bash
export PROJECT_ROOT=/home/maria/rca-project
python3 scripts/orchestration/run_experiment.py --help
```

## Verify and audit data

Verify one processed run against its saved run arguments:

```bash
python3 scripts/audit/verify_run.py \
  --csv results/new_dataset/processed_dataset/compose/example_cpu_throttle_compose_50rps.csv \
  --args results/new_dataset/raw_dataset/example_cpu_throttle_compose_50rps/args.txt
```

Audit the complete dataset (expects `ground_truth_manifest.jsonl`, `raw_dataset/`, and `processed_dataset/` beneath the dataset root):

```bash
python3 scripts/audit/audit_new_dataset.py results/new_dataset
```

The audit writes `results/new_dataset/audit_summary.json` and a Markdown report at `docs/new_dataset_audit.md` relative to the project root. A missing manifest or incomplete run will cause audit failures; do not treat an absent report or an incomplete dataset as a passing result.

## Stop services and protect data

```bash
bash deploy/down.sh
```

**Warning:** `down.sh` runs Docker Compose with `down -v`, which removes Compose-managed volumes as well as containers and networks. This can erase the benchmark's persisted database state. Back up any data you need before using it. To stop containers while retaining volumes, run the matching Compose command without `-v` from the DeathStarBench directory:

```bash
docker compose \
  -f docker-compose.yml \
  -f /home/maria/rca-project/deploy/docker-compose-telemetry.yml \
  down --remove-orphans
```

## Configuration and data notes

- `configs/campaign_matrix.yaml` describes the planned benchmark factors and smoke-test subset. The current `run_pilots.py` executes its own hard-coded pilot list; it does not load that YAML campaign matrix.
- Raw telemetry and processed CSVs are different artifacts. Keep raw runs and `ground_truth_manifest.jsonl` intact when auditing or reproducing results.
- The project does not currently provide a root-level package/build configuration, model-training entry point, or dataset download automation. Do not use the old README's `src/`, `paper/`, DVC, or `scripts/audit/run_all.sh` instructions; those paths/commands are not present in this checkout.
