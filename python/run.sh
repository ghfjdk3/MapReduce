#!/bin/bash
# ============================================================
# MapReduce 分布式运行脚本
# 启动 Master → 启动多个 Worker → 提交 Client 作业 → 清理
# ============================================================

set -e

MASTER_PORT=5000
WORKER_COUNT=3
BASE_WORKER_PORT=5001
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR"

# 检查是否安装了 requests 和 flask
python -c "import flask" 2>/dev/null || python -m pip install flask
python -c "import requests" 2>/dev/null || python -m pip install requests

echo "=========================================="
echo " MapReduce 分布式 WordCount 运行脚本"
echo "=========================================="
echo ""
echo "架构: 1 Master + ${WORKER_COUNT} Workers (每个双 Slot: Map + Reduce)"
echo ""

# 清理旧的临时文件
rm -f mapper.pkl reducer.pkl

# ---- 1. 启动 Master ----
echo "[启动] Master (端口 ${MASTER_PORT})..."
python main.py master --port ${MASTER_PORT} &
MASTER_PID=$!
sleep 2

# 检查 Master 是否启动成功
if ! kill -0 ${MASTER_PID} 2>/dev/null; then
    echo "[错误] Master 启动失败"
    exit 1
fi
echo "[OK] Master 已启动，PID=${MASTER_PID}"
echo ""

# ---- 2. 启动 Worker ----
WORKER_PIDS=()
for i in $(seq 1 ${WORKER_COUNT}); do
    PORT=$((BASE_WORKER_PORT + i - 1))
    echo "[启动] Worker ${i} (端口 ${PORT})..."
    python main.py worker --master localhost:${MASTER_PORT} --port ${PORT} &
    PID=$!
    WORKER_PIDS+=(${PID})
    sleep 1
done

echo ""
echo "[OK] ${WORKER_COUNT} 个 Worker 已启动"
echo ""

# 等待 Worker 全部注册完毕
echo "[等待] Worker 注册到 Master..."
sleep 3

# ---- 3. 提交作业 ----
echo ""
echo "[提交] WordCount 作业..."
python main.py client \
    --master localhost:${MASTER_PORT} \
    --input ../input.txt \
    --output ../output.txt
CLIENT_EXIT=$?

echo ""

# ---- 4. 清理所有进程 ----
echo "[清理] 停止所有进程..."

# 停止 Master
if kill -0 ${MASTER_PID} 2>/dev/null; then
    kill ${MASTER_PID} 2>/dev/null
    echo "[OK] Master (PID=${MASTER_PID}) 已停止"
fi

# 停止所有 Worker
for PID in "${WORKER_PIDS[@]}"; do
    if kill -0 ${PID} 2>/dev/null; then
        kill ${PID} 2>/dev/null
        echo "[OK] Worker (PID=${PID}) 已停止"
    fi
done

# 清理 pickle 文件
rm -f mapper.pkl reducer.pkl

echo ""
echo "=========================================="
if [ ${CLIENT_EXIT} -eq 0 ]; then
    echo " WordCount 执行成功！"
    echo " 结果文件: ../output.txt"
    if [ -f ../output.txt ]; then
        echo ""
        echo " 输出内容:"
        cat ../output.txt
    fi
else
    echo " WordCount 执行失败，退出码=${CLIENT_EXIT}"
fi
echo "=========================================="

exit ${CLIENT_EXIT}