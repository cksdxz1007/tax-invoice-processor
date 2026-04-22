#!/bin/bash

# 税务应收应付数据处理系统 - 启动脚本

# 配置
APP_NAME="tax-invoice-processor"
PYTHON_BIN="python"
APP_FILE="app.py"
LOG_FILE="app.log"
PID_FILE="app.pid"

# 生成随机5位数端口 (10000-99999)
generate_random_port() {
    echo $((RANDOM % 90000 + 10000))
}

DEFAULT_PORT=15090

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# 端口 (命令行参数 > 环境变量 > 默认值)
PORT=${2:-$PORT}
PORT=${PORT:-$DEFAULT_PORT}

# 目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 激活虚拟环境
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

show_help() {
    echo "用法: ./start.sh [命令] [端口]"
    echo ""
    echo "命令:"
    echo "  start     启动服务 (后台运行)"
    echo "  stop      停止服务"
    echo "  restart   重启服务"
    echo "  status    查看运行状态"
    echo "  log       查看日志"
    echo "  help      显示帮助"
    echo ""
    echo "端口:"
    echo "  默认随机5位数 (10000-99999)"
    echo "  可通过环境变量 PORT 指定"
    echo "  或命令行第二个参数指定"
    echo ""
    echo "示例:"
    echo "  ./start.sh start        # 启动(随机端口)"
    echo "  ./start.sh start 8080  # 启动(端口8080)"
    echo "  PORT=8080 ./start.sh start  # 启动(端口8080)"
}

start_server() {
    if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo -e "${YELLOW}服务已在运行 (PID: $(cat $PID_FILE))${NC}"
        return 1
    fi

    echo -e "${GREEN}启动服务 (端口: $PORT)...${NC}"

    # 后台启动，指定端口
    PORT=$PORT nohup $PYTHON_BIN $APP_FILE > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    sleep 2

    if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo -e "${GREEN}服务已启动 (PID: $(cat $PID_FILE))${NC}"
        echo -e "访问地址: http://127.0.0.1:$PORT"
        echo -e "日志文件: $LOG_FILE"
    else
        echo -e "${RED}启动失败，请查看日志: $LOG_FILE${NC}"
        rm -f "$PID_FILE"
    fi
}

stop_server() {
    if [ ! -f "$PID_FILE" ]; then
        echo -e "${YELLOW}服务未运行${NC}"
        return 1
    fi

    PID=$(cat "$PID_FILE")

    if ! kill -0 "$PID" 2>/dev/null; then
        echo -e "${YELLOW}服务未运行 (PID文件过期)${NC}"
        rm -f "$PID_FILE"
        return 1
    fi

    echo -e "${GREEN}停止服务 (PID: $PID)...${NC}"
    kill "$PID"

    # 等待进程结束
    for i in {1..5}; do
        if ! kill -0 "$PID" 2>/dev/null; then
            break
        fi
        sleep 1
    done

    # 强制终止
    if kill -0 "$PID" 2>/dev/null; then
        kill -9 "$PID" 2>/dev/null
    fi

    rm -f "$PID_FILE"
    echo -e "${GREEN}服务已停止${NC}"
}

show_status() {
    if [ ! -f "$PID_FILE" ]; then
        echo -e "${YELLOW}服务未运行${NC}"
        return 1
    fi

    PID=$(cat "$PID_FILE")

    if kill -0 "$PID" 2>/dev/null; then
        # 获取实际监听端口
        ACTUAL_PORT=$(lsof -p $PID 2>/dev/null | grep -i listen | awk '{print $9}' | grep -oE '[0-9]+$' | head -1)
        if [ -z "$ACTUAL_PORT" ]; then
            ACTUAL_PORT=$PORT
        fi
        echo -e "${GREEN}服务运行中 (PID: $PID, 端口: $ACTUAL_PORT)${NC}"
    else
        echo -e "${YELLOW}服务未运行 (PID文件过期)${NC}"
        rm -f "$PID_FILE"
    fi
}

show_log() {
    if [ ! -f "$LOG_FILE" ]; then
        echo -e "${YELLOW}日志文件不存在${NC}"
        return 1
    fi
    tail -50 "$LOG_FILE"
}

# 主逻辑
case "${1:-help}" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    restart)
        stop_server
        sleep 1
        start_server
        ;;
    status)
        show_status
        ;;
    log)
        show_log
        ;;
    help|*)
        show_help
        ;;
esac
