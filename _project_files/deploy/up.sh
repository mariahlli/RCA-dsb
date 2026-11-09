#!/usr/bin/env bash
# deploy/up.sh — Launch DeathStarBench Social Network with Prometheus, cAdvisor, Jaeger
set -euo pipefail

BENCH_DIR="/home/maria/DeathStarBench/socialNetwork"
DEPLOY_DIR="/home/maria/rca-project/deploy"

echo "[DEPLOY] Starting DeathStarBench Social Network + Telemetry Stack..."
cd "$BENCH_DIR"
docker compose -f docker-compose.yml -f "$DEPLOY_DIR/docker-compose-telemetry.yml" up -d

echo "[DEPLOY] Waiting for Nginx Web Server (port 8080) and Prometheus (port 9090)..."
for i in {1..30}; do
    if curl -s http://localhost:8080 >/dev/null 2>&1; then
        echo " [OK] Nginx is UP!"
        break
    fi
    echo -n "."
    sleep 2
done

for i in {1..30}; do
    if curl -s http://localhost:9090/-/ready >/dev/null 2>&1; then
        echo " [OK] Prometheus is UP!"
        break
    fi
    echo -n "."
    sleep 2
done

echo "[DEPLOY] Initializing Social Graph Database schema & users..."
docker run --rm --net=host -v "$BENCH_DIR":/app -w /app python:3.10-slim sh -c \
    "pip install --no-cache-dir aiohttp >/dev/null 2>&1 && python3 scripts/init_social_graph.py --graph=socfb-Reed98"

echo ""
echo "[DEPLOY] Stack successfully deployed and initialized."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | head -35
