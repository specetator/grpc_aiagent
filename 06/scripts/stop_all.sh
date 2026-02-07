#!/bin/bash
# Spark Push 一键停止脚本
# 更新日期: 2026-02-05

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

KAFKA_HOME="$HOME/3rd/kafka_2.13-3.7.0"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

# 停止应用服务
log_info "停止 Web Demo..."
pkill -f "web_demo_server" 2>/dev/null || true

log_info "停止 Job Server..."
pkill -f "job_server" 2>/dev/null || true

log_info "停止 Comet Server..."
pkill -f "comet_server" 2>/dev/null || true

log_info "停止 Logic Server..."
pkill -f "logic_server" 2>/dev/null || true

# 可选：停止Kafka和Zookeeper（默认不停止）
if [ "$1" == "--all" ]; then
    log_info "停止 Kafka..."
    "$KAFKA_HOME/bin/kafka-server-stop.sh" 2>/dev/null || true
    sleep 3
    
    log_info "停止 Zookeeper..."
    "$KAFKA_HOME/bin/zookeeper-server-stop.sh" 2>/dev/null || true
fi

sleep 1
echo ""
log_info "服务已停止"
echo ""
echo "提示: 使用 --all 参数可同时停止 Kafka 和 Zookeeper"
