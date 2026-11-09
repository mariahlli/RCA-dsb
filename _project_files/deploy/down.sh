#!/usr/bin/env bash
# deploy/down.sh — Tear down DeathStarBench Social Network and telemetry stack
set -euo pipefail

BENCH_DIR="/home/maria/DeathStarBench/socialNetwork"
DEPLOY_DIR="/home/maria/rca-project/deploy"

echo "[TEARDOWN] Stopping containers..."
cd "$BENCH_DIR"
docker compose -f docker-compose.yml -f "$DEPLOY_DIR/docker-compose-telemetry.yml" down -v --remove-orphans

echo "[TEARDOWN] Cleaned up."
