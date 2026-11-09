#!/bin/bash
set -e

SRC="/mnt/c/Users/EL ASSLI/OneDrive/Desktop/masterpfe/_project_files"
DST="/home/maria/rca-project"

mkdir -p "$DST/configs" "$DST/scripts/chaos" "$DST/scripts/collector" "$DST/scripts/orchestration" "$DST/scripts/preprocessing" "$DST/scripts/audit"

cp "$SRC/scripts/chaos/inject_fault.py" "$DST/scripts/chaos/"
cp "$SRC/scripts/collector/docker_net_sampler.py" "$DST/scripts/collector/"
cp "$SRC/scripts/collector/export_telemetry.py" "$DST/scripts/collector/"
cp "$SRC/scripts/orchestration/run_experiment.py" "$DST/scripts/orchestration/"
cp "$SRC/scripts/preprocessing/process_run.py" "$DST/scripts/preprocessing/"
cp "$SRC/scripts/audit/audit_new_dataset.py" "$DST/scripts/audit/"
cp "$SRC/scripts/audit/run_task7_pilots.py" "$DST/scripts/audit/"
cp "$SRC/configs/campaign_matrix.yaml" "$DST/configs/"

chmod +x "$DST/scripts/"*/*.py

echo "Files synced successfully:"
ls -la "$DST/scripts/chaos/inject_fault.py" "$DST/scripts/collector/docker_net_sampler.py" "$DST/scripts/preprocessing/process_run.py" "$DST/configs/campaign_matrix.yaml"
