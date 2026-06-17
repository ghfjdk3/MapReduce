"""MapReduce 分布式框架协议常量"""

import os

# Master API 路由
MASTER_REGISTER = "/register"
MASTER_SUBMIT_JOB = "/submit_job"
MASTER_MAP_DONE = "/map_done"
MASTER_REDUCE_DONE = "/reduce_done"
MASTER_JOB_STATUS = "/job_status"

# Worker API 路由
WORKER_EXECUTE_MAP = "/execute_map"
WORKER_EXECUTE_REDUCE = "/execute_reduce"
WORKER_GET_PARTITION = "/partition"
WORKER_NOTIFY_MAP_READY = "/notify_map_ready"
WORKER_PING = "/ping"

# JSON 字段名
FIELD_JOB_ID = "job_id"
FIELD_WORKER_PORT = "worker_port"
FIELD_INPUT_PATH = "input_path"
FIELD_OUTPUT_PATH = "output_path"
FIELD_MAPPER_PKL = "mapper_pkl"
FIELD_REDUCER_PKL = "reducer_pkl"
FIELD_LINES = "lines"
FIELD_SHARD = "shard"
FIELD_REDUCE_TASK = "reduce_task"
FIELD_REDUCE_RESULT = "reduce_result"
FIELD_PARTITION_ID = "partition_id"
FIELD_MAP_WORKERS = "map_workers"
FIELD_NUM_REDUCERS = "num_reducers"
FIELD_TOTAL_MAP_TASKS = "total_map_tasks"
FIELD_PARTITION_DATA = "partition_data"
FIELD_STATUS = "status"
FIELD_ERROR = "error"
FIELD_WORKER_ID = "worker_id"
FIELD_SLOT_TYPE = "slot_type"
FIELD_MAP_WORKER_INFO = "map_worker_info"

# 作业状态
STATUS_PENDING = "pending"
STATUS_MAP_RUNNING = "map_running"
STATUS_SHUFFLING = "shuffling"
STATUS_REDUCE_RUNNING = "reduce_running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

# 默认配置
DEFAULT_MASTER_PORT = 5000
DEFAULT_WORKER_PORT = 5001
MAP_REDUCE_PORT_OFFSET = 1000
DEFAULT_TIMEOUT = 30
OUTPUT_DELIMITER = "\t"
OUTPUT_LINE_END = os.linesep

# Slot 类型
SLOT_TYPE_MAP = "map"
SLOT_TYPE_REDUCE = "reduce"

# Reduce 提前触发阈值
MAP_PROGRESS_TRIGGER_RATIO = 0.1
