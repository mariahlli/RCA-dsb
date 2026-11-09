#!/usr/bin/env bash
set -euo pipefail

services=(
    "compose-post-service-1"
    "home-timeline-service-1"
    "text-service-1"
    "post-storage-service-1"
    "user-service-1"
    "social-graph-service-1"
)

echo "[SIDECARS] Pre-creating persistent netshoot sidecars..."
for svc in "${services[@]}"; do
    cname="socialnetwork-$svc"
    sname="${svc/-1/}"
    sidecar="netshoot-$sname"
    if docker ps --format '{{.Names}}' | grep -q "^${sidecar}$"; then
        echo "  [OK] $sidecar is already running"
    else
        docker rm -f "$sidecar" 2>/dev/null || true
        docker run -d --name "$sidecar" --net=container:"$cname" --cap-add=NET_ADMIN nicolaka/netshoot sleep infinity >/dev/null
        echo "  [CREATED] $sidecar attached to $cname"
    fi
done

echo "[SIDECARS] Status:"
docker ps --filter "name=netshoot" --format "table {{.Names}}\t{{.Status}}"
