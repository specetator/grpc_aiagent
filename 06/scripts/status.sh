#!/bin/bash
check_port() { netstat -tlnp 2>/dev/null | grep -q ":$2 " && echo "[OK] $1 ($2)" || echo "[NO] $1 ($2)"; }
echo "=== Spark Push Status ==="
check_port "Zookeeper" 2181; check_port "Kafka" 9092; check_port "MySQL" 3306; check_port "Redis" 6379
check_port "Logic HTTP" 9101; check_port "Logic gRPC" 9100; check_port "Comet WS" 9200; check_port "Comet gRPC" 9205; check_port "Web Demo" 9080
pgrep -f job_server >/dev/null && echo "[OK] Job Server" || echo "[NO] Job Server"
ps aux | grep _server | grep -v grep
