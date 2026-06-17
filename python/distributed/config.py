"""MapReduce 分布式框架可配置参数"""

# ---- 网络 ----
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000
WORKER_BASE_PORT = 5001
MAP_REDUCE_PORT_OFFSET = 1000
HTTP_TIMEOUT = 30

# ---- Worker 注册 ----
WORKER_REGISTER_RETRIES = 30
WORKER_REGISTER_RETRY_INTERVAL = 1  # 秒

# ---- MapReduce 调度 ----
MAP_PROGRESS_TRIGGER_RATIO = 0.1
MAP_PHASE_TIMEOUT = 120    # map 阶段总超时（秒）
REDUCE_PHASE_TIMEOUT = 120 # reduce 阶段总超时（秒）
NOTIFY_WAIT_TIMEOUT = 5    # reduce slot 等待初始化超时（秒）

# ---- 心跳 ----
HEARTBEAT_INTERVAL = 5     # Master 检查心跳间隔（秒）
HEARTBEAT_TIMEOUT = 15     # Worker 失联判定超时（秒）

# ---- 输出格式 ----
OUTPUT_DELIMITER = "\t"
OUTPUT_LINE_END = "\n"