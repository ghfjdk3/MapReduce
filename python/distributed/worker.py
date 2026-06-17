"""Worker 节点 — Map Slot + Reduce Slot"""

import base64
import hashlib
import pickle
import threading
import time
import sys
from typing import List, Dict, Any

from flask import Flask, request, jsonify

from .protocol import (
    WORKER_EXECUTE_MAP, WORKER_EXECUTE_REDUCE, WORKER_GET_PARTITION,
    WORKER_NOTIFY_MAP_READY, WORKER_PING,
    MASTER_REGISTER, MASTER_MAP_DONE, MASTER_REDUCE_DONE,
    FIELD_JOB_ID, FIELD_WORKER_PORT, FIELD_WORKER_ID,
    FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_LINES, FIELD_REDUCE_RESULT,
    FIELD_PARTITION_ID, FIELD_MAP_WORKER_INFO, FIELD_NUM_REDUCERS,
    FIELD_TOTAL_MAP_TASKS, FIELD_PARTITION_DATA,
    FIELD_SLOT_TYPE, FIELD_ERROR,
    SLOT_TYPE_MAP, SLOT_TYPE_REDUCE,
)
from .config import (
    MAP_REDUCE_PORT_OFFSET, WORKER_REGISTER_RETRIES,
    WORKER_REGISTER_RETRY_INTERVAL, NOTIFY_WAIT_TIMEOUT,
)
from .network import post_json, get_json, make_url


class Worker:
    """管理双 Slot 的 Worker 进程入口"""

    def __init__(self, master_host: str, master_port: int, base_port: int):
        self.master_host = master_host
        self.master_port = master_port
        self.map_port = base_port
        self.reduce_port = base_port + MAP_REDUCE_PORT_OFFSET

    def run(self):
        map_slot = MapSlot(self.master_host, self.master_port, self.map_port)
        reduce_slot = ReduceSlot(self.master_host, self.master_port, self.reduce_port)

        t1 = threading.Thread(target=map_slot.run, daemon=True)
        t2 = threading.Thread(target=reduce_slot.run, daemon=True)
        t1.start()
        t2.start()

        print(f"[Worker] Map Slot :{self.map_port} + Reduce Slot :{self.reduce_port}")
        t1.join()
        t2.join()


# ================================================================
# Map Slot
# ================================================================

class MapSlot:
    def __init__(self, master_host: str, master_port: int, port: int):
        self.master_host = master_host
        self.master_port = master_port
        self.port = port
        self.worker_id = ""
        self._partitions: Dict[str, Dict[int, List[List[Any]]]] = {}
        self._lock = threading.Lock()

    def run(self):
        app = self._create_app()
        threading.Thread(target=self._register_loop, daemon=True).start()
        app.run(host="0.0.0.0", port=self.port, threaded=True)

    def _create_app(self):
        app = Flask(__name__)

        @app.route(WORKER_EXECUTE_MAP, methods=["POST"])
        def execute_map():
            return self._handle_map(request)

        @app.route(WORKER_GET_PARTITION + "/<job_id>/<int:partition_id>", methods=["GET"])
        def get_partition(job_id, partition_id):
            return self._handle_get_partition(job_id, partition_id)

        @app.route(WORKER_PING, methods=["GET"])
        def ping():
            return jsonify({"status": "ok"})

        return app

    def _register_loop(self):
        for i in range(WORKER_REGISTER_RETRIES):
            try:
                url = make_url(self.master_host, self.master_port, MASTER_REGISTER)
                resp = post_json(url, {
                    FIELD_WORKER_PORT: self.port,
                    FIELD_SLOT_TYPE: SLOT_TYPE_MAP,
                })
                self.worker_id = resp.get(FIELD_WORKER_ID, "")
                print(f"[Map Slot :{self.port}] 注册成功, id={self.worker_id}")
                return
            except Exception as e:
                print(f"[Map Slot :{self.port}] 注册失败 ({i+1}/{WORKER_REGISTER_RETRIES}): {e}")
                time.sleep(WORKER_REGISTER_RETRY_INTERVAL)
        sys.exit(1)

    def _handle_map(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        mapper_pkl_b64 = data.get(FIELD_MAPPER_PKL, "")
        reducer_pkl_b64 = data.get(FIELD_REDUCER_PKL, "")
        lines = data.get(FIELD_LINES, [])
        num_reducers = data.get(FIELD_NUM_REDUCERS, 1)

        try:
            mapper_cls = pickle.loads(base64.b64decode(mapper_pkl_b64))
            mapper = mapper_cls()
            partitions: Dict[int, List[List[Any]]] = {i: [] for i in range(num_reducers)}

            for line in lines:
                for key, value in mapper.map(line):
                    p = int(hashlib.md5(str(key).encode()).hexdigest(), 16) % num_reducers
                    partitions[p].append([key, value])

            raw_total = sum(len(v) for v in partitions.values())

            # Combine: 对每个 partition 本地归并相同 key
            if reducer_pkl_b64:
                reducer_cls = pickle.loads(base64.b64decode(reducer_pkl_b64))
                reducer = reducer_cls()
                for pid in range(num_reducers):
                    if partitions[pid]:
                        grouped: Dict[Any, List] = {}
                        for key, value in partitions[pid]:
                            grouped.setdefault(key, []).append(value)
                        partitions[pid] = []
                        for key, vals in grouped.items():
                            r_key, r_value = reducer.reduce(key, vals)
                            partitions[pid].append([r_key, r_value])

            combined_total = sum(len(v) for v in partitions.values())
            print(f"[Map Slot :{self.port}] map 完成: {raw_total} 对 → combine: {combined_total} 对")

            with self._lock:
                self._partitions[job_id] = partitions

            self._send_map_done(job_id, combined_total)
            return jsonify({"ok": True, "pair_count": combined_total})
        except Exception as e:
            return jsonify({FIELD_ERROR: str(e), "ok": False}), 500

    def _handle_get_partition(self, job_id: str, partition_id: int):
        with self._lock:
            data = self._partitions.get(job_id, {}).get(partition_id, [])
        return jsonify({FIELD_PARTITION_DATA: list(data)})

    def _send_map_done(self, job_id: str, pair_count: int):
        try:
            post_json(make_url(self.master_host, self.master_port, MASTER_MAP_DONE), {
                FIELD_JOB_ID: job_id,
                FIELD_WORKER_ID: self.worker_id,
                "pair_count": pair_count,
            })
        except Exception as e:
            print(f"[Map Slot :{self.port}] map_done 回传失败: {e}")


# ================================================================
# Reduce Slot
# ================================================================

class ReduceSlot:
    def __init__(self, master_host: str, master_port: int, port: int):
        self.master_host = master_host
        self.master_port = master_port
        self.port = port
        self.worker_id = ""
        self._shards: Dict[str, Dict[int, List[List[Any]]]] = {}
        self._total_map_tasks: Dict[str, int] = {}
        self._ready_map_workers: Dict[str, set] = {}
        self._partition_id: Dict[str, int] = {}
        self._reducer_pkl_b64: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._done = set()

    def run(self):
        app = self._create_app()
        threading.Thread(target=self._register_loop, daemon=True).start()
        app.run(host="0.0.0.0", port=self.port, threaded=True)

    def _create_app(self):
        app = Flask(__name__)

        @app.route(WORKER_EXECUTE_REDUCE, methods=["POST"])
        def execute_reduce():
            return self._handle_execute_reduce(request)

        @app.route(WORKER_NOTIFY_MAP_READY, methods=["POST"])
        def notify_map_ready():
            return self._handle_notify(request)

        @app.route(WORKER_PING, methods=["GET"])
        def ping():
            return jsonify({"status": "ok"})

        return app

    def _register_loop(self):
        for i in range(WORKER_REGISTER_RETRIES):
            try:
                url = make_url(self.master_host, self.master_port, MASTER_REGISTER)
                resp = post_json(url, {
                    FIELD_WORKER_PORT: self.port,
                    FIELD_SLOT_TYPE: SLOT_TYPE_REDUCE,
                })
                self.worker_id = resp.get(FIELD_WORKER_ID, "")
                print(f"[Reduce Slot :{self.port}] 注册成功, id={self.worker_id}")
                return
            except Exception as e:
                print(f"[Reduce Slot :{self.port}] 注册失败 ({i+1}/{WORKER_REGISTER_RETRIES}): {e}")
                time.sleep(WORKER_REGISTER_RETRY_INTERVAL)
        sys.exit(1)

    def _handle_execute_reduce(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        reducer_pkl_b64 = data.get(FIELD_REDUCER_PKL, "")
        partition_id = data.get(FIELD_PARTITION_ID, 0)
        total_map_tasks = data.get(FIELD_TOTAL_MAP_TASKS, 0)

        print(f"[Reduce Slot :{self.port}] 收到 reduce 任务: job={job_id}, partition={partition_id}")

        with self._lock:
            self._shards[job_id] = self._shards.get(job_id, {})
            self._shards[job_id][partition_id] = []
            self._total_map_tasks[job_id] = total_map_tasks
            self._ready_map_workers[job_id] = set()
            self._partition_id[job_id] = partition_id
            self._reducer_pkl_b64[job_id] = reducer_pkl_b64

        return jsonify({"ok": True})

    def _handle_notify(self, req):
        """Master 通知：某个 map worker 的数据已就绪"""
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID, "")
        map_worker_info = data.get(FIELD_MAP_WORKER_INFO, {})

        # 等待 execute_reduce 初始化完成
        deadline = time.time() + NOTIFY_WAIT_TIMEOUT
        while time.time() < deadline:
            with self._lock:
                if job_id in self._ready_map_workers:
                    break
            time.sleep(0.1)

        should_final = False
        with self._lock:
            if job_id not in self._ready_map_workers:
                return jsonify({"ok": True})
            wid = str(map_worker_info)
            if wid in self._ready_map_workers[job_id]:
                return jsonify({"ok": True})
            self._ready_map_workers[job_id].add(wid)
            total = self._total_map_tasks.get(job_id, 1)
            if len(self._ready_map_workers[job_id]) >= total and job_id not in self._done:
                self._done.add(job_id)
                should_final = True

        # 拉取 partition 数据
        partition_id = self._partition_id.get(job_id, 0)
        try:
            url = make_url(map_worker_info["host"], map_worker_info["port"],
                           WORKER_GET_PARTITION + "/" + job_id + "/" + str(partition_id))
            resp = get_json(url)
            pd = resp.get(FIELD_PARTITION_DATA, [])
        except Exception as e:
            print(f"[Reduce Slot :{self.port}] 拉取失败 {map_worker_info}: {e}")
            pd = []

        with self._lock:
            self._shards[job_id][partition_id].extend(pd)

        print(f"[Reduce Slot :{self.port}] 拉取 map_worker={map_worker_info['host']}:{map_worker_info['port']}, "
              f"part={partition_id}, got={len(pd)}")

        if should_final:
            self._do_final_reduce(job_id)

        return jsonify({"ok": True})

    def _do_final_reduce(self, job_id: str):
        """所有 map worker 数据到位，执行 shuffle + reduce"""
        print(f"[Reduce Slot :{self.port}] 所有 map 数据到位，开始 shuffle+reduce")

        partition_id = self._partition_id.get(job_id, 0)
        all_shards = self._shards.get(job_id, {}).get(partition_id, [])

        # shuffle: 按 key 分组
        grouped: Dict[Any, List] = {}
        for pair in all_shards:
            key, value = pair[0], pair[1]
            grouped.setdefault(key, []).append(value)

        sorted_keys = sorted(grouped.keys(), key=lambda k: str(k))

        # reduce
        reducer_pkl_b64 = self._reducer_pkl_b64.get(job_id, "")
        reducer_cls = pickle.loads(base64.b64decode(reducer_pkl_b64))
        reducer = reducer_cls()

        result: List[List[Any]] = []
        for key in sorted_keys:
            r_key, r_value = reducer.reduce(key, grouped[key])
            result.append([r_key, r_value])

        print(f"[Reduce Slot :{self.port}] reduce 完成: {len(result)} 个结果")

        # 回传 Master
        try:
            post_json(make_url(self.master_host, self.master_port, MASTER_REDUCE_DONE), {
                FIELD_JOB_ID: job_id,
                FIELD_WORKER_ID: self.worker_id,
                FIELD_REDUCE_RESULT: result,
            })
        except Exception as e:
            print(f"[Reduce Slot :{self.port}] reduce_done 回传失败: {e}")

        # 清理
        with self._lock:
            self._shards.pop(job_id, None)
            self._ready_map_workers.pop(job_id, None)
            self._partition_id.pop(job_id, None)
            self._reducer_pkl_b64.pop(job_id, None)
            self._total_map_tasks.pop(job_id, None)


def run_worker(master_host: str, master_port: int, port: int):
    Worker(master_host, master_port, port).run()