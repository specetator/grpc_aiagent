#!/bin/bash
# Spark Push 一键启动脚本
# 更新日期: 2026-02-05

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 路径配置
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
CONF_DIR="$PROJECT_ROOT/conf"
KAFKA_HOME="$HOME/3rd/kafka_2.13-3.7.0"
KAFKA_DATA="$HOME/kafka_data"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_port() {
    local port=$1
    local name=$2
    if netstat -tlnp 2>/dev/null | grep -q ":$port "; then
        log_info "$name 已在端口 $port 运行"
        return 0
    fi
    return 1
}

wait_for_port() {
    local port=$1
    local name=$2
    local max_wait=30
    local count=0
    while ! netstat -tlnp 2>/dev/null | grep -q ":$port "; do
        sleep 1
        count=$((count + 1))
        if [ $count -ge $max_wait ]; then
            log_error "$name 启动超时 (端口 $port)"
            return 1
        fi
    done
    log_info "$name 启动成功 (端口 $port)"
}

# 1. 启动 Zookeeper
start_zookeeper() {
    if check_port 2181 "Zookeeper"; then
        return 0
    fi
    log_info "启动 Zookeeper..."
    mkdir -p "$KAFKA_DATA/zookeeper"
    "$KAFKA_HOME/bin/zookeeper-server-start.sh" -daemon "$KAFKA_HOME/config/zookeeper.properties"
    wait_for_port 2181 "Zookeeper"
}

# 2. 启动 Kafka
start_kafka() {
    if check_port 9092 "Kafka"; then
        return 0
    fi
    log_info "启动 Kafka..."
    mkdir -p "$KAFKA_DATA/kafka-logs"
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$KAFKA_HOME/config/server.properties"
    wait_for_port 9092 "Kafka"
}

# 3. 启动 Logic Server
start_logic() {
    if pgrep -f "logic_server" > /dev/null; then
        log_info "Logic Server 已运行"
        return 0
    fi
    log_info "启动 Logic Server..."
    cd "$BUILD_DIR"
    nohup ./logic/logic_server "$CONF_DIR/logic.conf" > /dev/null 2>&1 &
    sleep 3
    wait_for_port 9101 "Logic HTTP"
    wait_for_port 9100 "Logic gRPC"
}

# 4. 启动 Comet Server
start_comet() {
    if pgrep -f "comet_server" > /dev/null; then
        log_info "Comet Server 已运行"
        return 0
    fi
    log_info "启动 Comet Server..."
    cd "$BUILD_DIR"
    nohup ./comet/comet_server "$CONF_DIR/comet.conf" > /dev/null 2>&1 &
    sleep 3
    wait_for_port 9200 "Comet WebSocket"
    wait_for_port 9205 "Comet gRPC"
}

# 5. 启动 Job Server
start_job() {
    if pgrep -f "job_server" > /dev/null; then
        log_info "Job Server 已运行"
        return 0
    fi
    log_info "启动 Job Server..."
    cd "$BUILD_DIR"
    nohup ./job/job_server "$CONF_DIR/job.conf" > /dev/null 2>&1 &
    sleep 2
    if pgrep -f "job_server" > /dev/null; then
        log_info "Job Server 启动成功"
    else
        log_error "Job Server 启动失败"
        return 1
    fi
}

# 6. 启动 Web Demo
start_web_demo() {
    if pgrep -f "web_demo_server" > /dev/null; then
        log_info "Web Demo 已运行"
        return 0
    fi
    log_info "启动 Web Demo..."
    cd "$BUILD_DIR"
    nohup ./web_demo/web_demo_server 9080 "$PROJECT_ROOT/web_demo/static" > /dev/null 2>&1 &
    sleep 2
    wait_for_port 9080 "Web Demo"
}

# 主流程
main() {
    echo "========================================"
    echo "       Spark Push 一键启动"
    echo "========================================"
    echo ""
    
    # 检查编译产物
    if [ ! -f "$BUILD_DIR/logic/logic_server" ]; then
        log_error "找不到编译产物，请先执行 cmake --build build"
        exit 1
    fi
    
    # 按顺序启动
    start_zookeeper
    start_kafka
    sleep 2
    start_logic
    start_comet
    start_job
    start_web_demo
    
    echo ""
    echo "========================================"
    echo "         启动完成!"
    echo "========================================"
    echo ""
    echo "服务端口:"
    echo "  - Web Demo:    http://localhost:9080"
    echo "  - Logic HTTP:  http://localhost:9101"
    echo "  - Comet WS:    ws://localhost:9200"
    echo ""
    echo "日志目录: $BUILD_DIR/logs/"
    echo ""
}

main "$@"
