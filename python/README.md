# MapReduce 分布式框架（Python 版，HTTP 协议）

基于 Python 实现的分布式 MapReduce 框架，支持单机和分布式两种运行模式。Master 和 Worker 以 HTTP 服务形式运行，通过 REST API 协同完成 MapReduce 作业。

---

## 项目结构

```
python/
├── mapper.py                  # Mapper 抽象基类（定义 map 接口）
├── reducer.py                 # Reducer 抽象基类（定义 reduce 接口）
├── wordcount_mapper.py        # WordCount 的 map 用户自定义函数
├── wordcount_reducer.py       # WordCount 的 reduce 用户自定义函数
├── save.py                    # pickle 序列化工具（将 UDF 保存为 .pkl 文件）
│
├── main.py                    # 统一入口（standalone / master / worker / client）
├── run.sh                     # 分布式一键启动脚本
│
└── distributed/               # 分布式框架核心包
    ├── __init__.py
    ├── master.py              # Master 节点（JobTracker）HTTP 服务
    ├── worker.py              # Worker 节点（TaskTracker）HTTP 服务
    ├── client.py              # Client 节点（作业提交客户端）
    ├── protocol.py            # 协议常量定义（API 路由、字段名、状态值）
    └── network.py             # 网络通信工具（HTTP 请求封装、错误处理）
```

## 架构概览

```
                        ┌─────────────────┐
                   ┌───▶│   Worker 1      │
                   │    │   (TaskTracker)  │
                   │    │   :5001          │
                   │    └─────────────────┘
┌─────────┐  /submit_job ┌─────────────────┐    ┌──────────┐
│  Client │─────────────▶│   Master        │───▶│ Worker 2 │
│         │              │   (JobTracker)  │    │ :5002    │
└─────────┘              │   :5000         │    └──────────┘
                         └─────────────────┘
                              ▲  │    │
                 /register ────┘  │    └─────── /map_done、/reduce_done
                 /ping            │
                              /execute_map、/execute_reduce
```

### 角色说明

| 角色 | 文件 | 职责 |
|------|------|------|
| **Master (JobTracker)** | `distributed/master.py` | Worker 注册管理、作业调度、任务分配、Shuffle 分组排序、结果汇总 |
| **Worker (TaskTracker)** | `distributed/worker.py` | 注册到 Master、接收并执行 Map/Reduce 子任务、回传结果 |
| **Client** | `distributed/client.py` | 提交 MapReduce 作业（携带 UDF 和输入/输出路径）、等待结果 |

---

## HTTP API

### Master API（端口 5000）

| 方法 | 路径 | 调用方 | 说明 |
|------|------|--------|------|
| `POST` | `/register` | Worker | Worker 注册到 Master，携带 `worker_port` |
| `POST` | `/submit_job` | Client | 提交 MapReduce 作业 |
| `POST` | `/map_done` | Worker | Worker 回传 map 阶段的中间结果 |
| `POST` | `/reduce_done` | Worker | Worker 回传 reduce 阶段的结果 |
| `GET` | `/job_status/<job_id>` | Client | 查询作业执行状态 |

### Worker API（端口 5001+）

| 方法 | 路径 | 调用方 | 说明 |
|------|------|--------|------|
| `POST` | `/execute_map` | Master | Master 下发 map 子任务（UDF + 数据分片） |
| `POST` | `/execute_reduce` | Master | Master 下发 reduce 子任务（UDF + key 分组数据） |
| `GET` | `/ping` | Master | 健康检查 |

### 请求/响应数据格式

- 所有请求/响应均使用 `Content-Type: application/json`
- UDF（用户自定义函数）通过 pickle 序列化后使用 base64 编码传输
- Map 中间结果格式：`[[key, value], [key, value], ...]`
- Reduce 输入格式：`[[key, [v1, v2, ...]], ...]`
- Reduce 输出格式：`[[key, value], ...]`

---

## 执行流程

```
启动阶段         注册阶段         Map 阶段       Shuffle 阶段     Reduce 阶段      输出阶段
   │               │               │               │               │               │
 Master ──── Master 监听 ──── 接收 Worker ──── 读取输入 ──── 收集 map ──── 均分 reduce ──── 写入
   │               │               │               │               │               │    输出文件
Worker  ──── 启动 ──── 注册 ──── 执行 map ──── 回传结果 ──── 执行 reduce ──── 回传结果
   │               │               │               │               │               │
Client  ──── ... ──── ... ──── 提交作业 ──── 等待... ──── 等待... ──── 等待... ──── 读取结果
```

### 详细步骤

1. **启动 & 注册**
   - Master 启动 HTTP 服务（默认 `0.0.0.0:5000`）
   - Worker 启动 HTTP 服务后，向 Master 发送 `POST /register` 注册
   - Master 为每个 Worker 分配唯一 `worker_id`

2. **提交作业**
   - Client 使用 `save.py` 将 WordCountMapper/Reducer 序列化为 `.pkl` 文件
   - Client 将 `.pkl` 文件 base64 编码后通过 `POST /submit_job` 提交给 Master
   - Master 为作业分配 `job_id`，立即返回，后台线程异步执行

3. **Map 阶段**
   - Master 读取输入文件，将行列表均分为 N 份（N = 已注册 Worker 数量）
   - 通过 `POST /execute_map` 将每份数据 + mapper UDF 发送给对应 Worker
   - Worker 反序列化 UDF，执行 `mapper.map(line)`，得到 `[(key, value), ...]`
   - Worker 通过 `POST /map_done` 将中间结果回传给 Master

4. **Shuffle 阶段**
   - Master 收集所有 Worker 的 map 结果
   - 按 key 分组（相同 key 的 values 合并为列表）
   - 按 key 字符串排序
   - 将分组结果均分为 N 份 reduce 任务

5. **Reduce 阶段**
   - Master 通过 `POST /execute_reduce` 下发 reduce 任务给各 Worker
   - Worker 反序列化 UDF，执行 `reducer.reduce(key, values)`，得到 `(key, value)`
   - Worker 通过 `POST /reduce_done` 将结果回传给 Master

6. **输出**
   - Master 汇总所有 reduce 结果，写入输出文件（格式：`key\tvalue`，每行一个）
   - Client 轮询 `GET /job_status/<job_id>` 直到状态变为 `completed`
   - Client 读取并显示输出文件内容

---

## 运行方式

### 环境要求

- Python 3.12+
- 依赖：`flask`、`requests`（`run.sh` 会自动安装）

### 一键运行（推荐）

```bash
cd python
sh run.sh
```

脚本自动完成：启动 Master → 启动 3 个 Worker → 注册等待 → 提交 WordCount 作业 → 清理进程。

### 手动分步运行

```bash
# 终端 1：启动 Master
cd python
python main.py master --port 5000

# 终端 2：启动 Worker 1
python main.py worker --master localhost:5000 --port 5001

# 终端 3：启动 Worker 2
python main.py worker --master localhost:5000 --port 5002

# 终端 4：启动 Worker 3
python main.py worker --master localhost:5000 --port 5003

# 终端 5：提交作业
python main.py client --master localhost:5000 --input ../input.txt --output ../output.txt
```

### 单机版（原版）

```bash
# 序列化 UDF
python save.py

# 运行单机版 MapReduce
python main.py standalone ../input.txt ../output.txt ./mapper.pkl ./reducer.pkl
```

---

## 可编程接口

用户可通过继承 `Mapper` 和 `Reducer` 基类实现自定义 MapReduce 作业：

```python
# wordcount_mapper.py
from mapper import Mapper
from typing import List, Tuple

class WordCountMapper(Mapper):
    def map(self, line: str) -> List[Tuple[str, int]]:
        words = line.split(" ")
        return [(word, 1) for word in words]
```

```python
# wordcount_reducer.py
from reducer import Reducer
from typing import List

class WordCountReducer(Reducer):
    def reduce(self, key: str, values: List[int]) -> tuple:
        return (key, sum(values))
```

---

## 关键设计

| 项目 | 方案 |
|------|------|
| **通信协议** | HTTP + JSON |
| **UDF 传输** | pickle 序列化 → base64 编码 → JSON 字段 |
| **Worker 发现** | Worker 启动时指定 `--master` 地址，主动 POST `/register` |
| **任务分配** | 均分策略：map 按行数 / Worker 数，reduce 按 key 分组数 / Worker 数 |
| **并发调度** | Master 使用线程并行向各 Worker 下发任务 |
| **状态跟踪** | 作业状态流转：`pending → map_running → shuffling → reduce_running → completed` |
| **基本容错** | 超时检测、任务状态记录 |

---

## 测试数据

输入文件 `../input.txt`：
```
a bb cc
a bb cc
d e ff
```

预期输出 `output.txt`：
```
a	2
bb	2
cc	2
d	1
e	1
ff	1
```
