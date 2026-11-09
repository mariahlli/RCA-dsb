#!/bin/bash
set -e

SRC="/home/maria/rca-project"
DST="/mnt/c/Users/EL ASSLI/OneDrive/Desktop/masterpfe"

mkdir -p "$DST/results/new_dataset/processed_dataset/mixed" "$DST/results/new_dataset/processed_dataset/compose" "$DST/results/new_dataset/processed_dataset/home" "$DST/docs"

cp -u "$SRC/results/new_dataset/processed_dataset/mixed/"*.csv "$DST/results/new_dataset/processed_dataset/mixed/"
cp -u "$SRC/results/new_dataset/processed_dataset/compose/"*.csv "$DST/results/new_dataset/processed_dataset/compose/"
cp -u "$SRC/results/new_dataset/processed_dataset/home/"*.csv "$DST/results/new_dataset/processed_dataset/home/"
cp "$SRC/results/new_dataset/audit_summary.json" "$DST/results/new_dataset/"
cp "$SRC/results/new_dataset/ground_truth_manifest.jsonl" "$DST/results/new_dataset/"
cp "$SRC/docs/new_dataset_audit.md" "$DST/docs/"

echo "Sync to Windows completed."
