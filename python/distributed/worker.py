"""Worker 节点 (TaskTracker) — Map 端分区 + Reduce 端拉取"""

import base64
import hashlib
import pickle
import threading
import time
import sys
from typing import List, Dict, Any

from flask import Flask, request, jsonify

from .protocol import (
    WORKER_EXECUTE_MAP, WORKER_EXECUTE_REDUCE, WORKER_GET_PARTITION, WORKER_PING,
    MASTER_REGISTER, MASTER_MAP_DONE, MASTER_REDUCE_DONE,
    FIELD_JOB_ID, FIELD_WORKER_PORT, FIELD_WORKER_ID,
    FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_LINES, FIELD_SHARD, FIELD_REDUCE_RESULT,
    FIELD_PARTITION_ID, FIELD_MAP_WORKERS, FIELD_NUM_REDUCERS,
    FIELD_PARTITION_DATA, FIELD_ERROR,
)
from .network import post_json, get_json, make_url


class Worker:
    """Worker 节点，提供 HTTP 服务，执行 Map/Reduce 子任务"""

    def __init__(self, master_host: str, master_port: int, port: int = 5001):
        self.master_host = master_host
        self.master_port = master_port
        self.port = port
        self.worker_id: str = ""
        self._registered = False
        self._partitions: Dict[str, Dict[int, List[List[Any]]]] = {}
        self._lock = threading.Lock()

    def run(self):
        app = self._create_app()

        def register_loop():
            time.sleep(0.5)
            self._register()

        threading.Thread(target=register_loop, daemon=True).start()
        print(f"[Worker] 启动于 http://0.0.0.0:{self.port}，Master: {self.master_host}:{self.master_port}")
        app.run(host="0.0.0.0", port=self.port, threaded=True)

    def _create_app(self):
        from flask import Flask
        app = Flask(__name__)

        @app.route(WORKER_EXECUTE_MAP, methods=["POST"])
        def execute_map():
            return self._handle_execute_map(request)

        @app.route(WORKER_EXECUTE_REDUCE, methods=["POST"])
        def execute_reduce():
            return self._handle_execute_reduce(request)

        @app.route(WORKER_GET_PARTITION + "/<job_id>/<int:partition_id>", methods=["GET"])
        def get_partition(job_id, partition_id):
            return self._handle_get_partition(job_id, partition_id)

        @app.route(WORKER_PING, methods=["GET"])
        def ping():
            return jsonify({"status": "ok", FIELD_WORKER_ID: self.worker_id})

        return app

    def _register(self):
        max_retries = 30
        for i in range(max_retries):
            try:
                url = make_url(self.master_host, self.master_port, MASTER_REGISTER)
                resp = post_json(url, {FIELD_WORKER_PORT: self.port})
                self.worker_id = resp.get(FIELD_WORKER_ID, "")
                self._registered = True
                print(f"[Worker] 注册成功，worker_id={self.worker_id}")
                return
            except Exception as e:
                print(f"[Worker] 注册失败 ({i+1}/{max_retries}): {e}")
                time.sleep(1)
        print("[Worker] 无法注册到 Master")
        sys.exit(1)

    # ================================================================
    # Partition 提供
    # ================================================================

    def _handle_get_partition(self, job_id: str, partition_id: int):
        """Reduce Worker 拉取指定 partition 的数据"""
        with self._lock:
            job_parts = self._partitions.get(job_id, {})
            data = job_parts.get(partition_id, [])
        return jsonify({
            FIELD_JOB_ID: job_id,
            FIELD_PARTITION_ID: partition_id,
            FIELD_PARTITION_DATA: list(data),
        })

    # ================================================================
    # Map 任务处理
    # ================================================================

    def _handle_execute_map(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        mapper_pkl_b64 = data.get(FIELD_MAPPER_PKL, "")
        lines = data.get(FIELD_LINES, [])
        num_reducers = data.get(FIELD_NUM_REDUCERS, 1)

        print(f"[Worker {self.worker_id}] 收到 map 任务: job={job_id}, lines={len(lines)}, reducers={num_reducers}")

        try:
            mapper_cls = pickle.loads(base64.b64decode(mapper_pkl_b64))
            mapper = mapper_cls()

            # 初始化 R 个 partition
            partitions: Dict[int, List[List[Any]]] = {i: [] for i in range(num_reducers)}

            for line in lines:
                pairs = mapper.map(line)
                for key, value in pairs:
                    p = int(hashlib.md5(str(key).encode()).hexdigest(), 16) % num_reducers
                    partitions[p].append([key, value])

            total_pairs = sum(len(v) for v in partitions.values())
            print(f"[Worker {self.worker_id}] map 完成: {total_pairs} 对, 分为 {num_reducers} 个 partition")

            # 存储 partition
            with self._lock:
                self._partitions[job_id] = partitions

            # 通知 Master
            self._send_map_done(job_id, total_pairs)
            return jsonify({"ok": True, "pair_count": total_pairs})

        except Exception as e:
            error_msg = f"map 任务执行失败: {str(e)}"
            print(f"[Worker {self.worker_id}] {error_msg}")
            return jsonify({FIELD_ERROR: error_msg, "ok": False}), 500

    # ================================================================
    # Reduce 任务处理（pull 模式）
    # ================================================================

    def _handle_execute_reduce(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        reducer_pkl_b64 = data.get(FIELD_REDUCER_PKL, "")
        partition_id = data.get(FIELD_PARTITION_ID, 0)
        map_workers = data.get(FIELD_MAP_WORKERS, [])

        print(f"[Worker {self.worker_id}] 收到 reduce 任务: job={job_id}, partition={partition_id}, "
              f"从 {len(map_workers)} 个 map worker 拉取")

        try:
            # 从所有 map worker 拉取属于本 partition 的数据
            shards: List[List[Any]] = []
            for mw in map_workers:
                try:
                    url = make_url(mw["host"], mw["port"],
                                   WORKER_GET_PARTITION + "/" + job_id + "/" + str(partition_id))
                    resp = get_json(url)
                    shards.extend(resp.get(FIELD_PARTITION_DATA, []))
                except Exception as e:
                    print(f"[Worker {self.worker_id}] 拉取 partition 失败 {mw['host']}:{mw['port']}: {e}")

            print(f"[Worker {self.worker_id}] 拉取完成，共 {len(shards)} 对")

            # Shuffle：按 key 分组
            grouped: Dict[Any, List] = {}
            for pair in shards:
                key, value = pair[0], pair[1]
                grouped.setdefault(key, []).append(value)

            # 按 key 排序
            sorted_keys = sorted(grouped.keys(), key=lambda k: str(k))

            # Reduce
            reducer_cls = pickle.loads(base64.b64decode(reducer_pkl_b64))
            reducer = reducer_cls()
            result: List[List[Any]] = []
            for key in sorted_keys:
                r_key, r_value = reducer.reduce(key, grouped[key])
                result.append([r_key, r_value])

            print(f"[Worker {self.worker_id}] reduce 完成: {len(result)} 个结果")
            self._send_reduce_done(job_id, result)
            return jsonify({"ok": True, "pair_count": len(result)})

        except Exception as e:
            error_msg = f"reduce 任务执行失败: {str(e)}"
            print(f"[Worker {self.worker_id}] {error_msg}")
            return jsonify({FIELD_ERROR: error_msg, "ok": False}), 500

    # ================================================================
    # 结果回传
    # ================================================================

    def _send_map_done(self, job_id: str, pair_count: int):
        url = make_url(self.master_host, self.master_port, MASTER_MAP_DONE)
        try:
            resp = post_json(url, {
                FIELD_JOB_ID: job_id,
                FIELD_WORKER_ID: self.worker_id,
                "pair_count": pair_count,
            })
            print(f"[Worker {self.worker_id}] map_done 已回传")
        except Exception as e:
            print(f"[Worker {self.worker_id}] map_done 回传失败: {e}")

    def _send_reduce_done(self, job_id: str, result: List[List]):
        url = make_url(self.master_host, self.master_port, MASTER_REDUCE_DONE)
        try:
            resp = post_json(url, {
                FIELD_JOB_ID: job_id,
                FIELD_WORKER_ID: self.worker_id,
                FIELD_REDUCE_RESULT: result,
            })
            print(f"[Worker {self.worker_id}] reduce_done 已回传")
        except Exception as e:
            print(f"[Worker {self.worker_id}] reduce_done 回传失败: {e}")


def run_worker(master_host: str, master_port: int, port: int):
    worker = Worker(master_host=master_host, master_port=master_port, port=port)
    worker.run()