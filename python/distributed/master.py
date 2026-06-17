"""Master 节点 (JobTracker)"""

import os
import uuid
import threading
import time
from typing import List, Dict, Any, Optional

from flask import Flask, request, jsonify

from .protocol import (
    MASTER_REGISTER, MASTER_SUBMIT_JOB, MASTER_MAP_DONE, MASTER_REDUCE_DONE,
    MASTER_JOB_STATUS, WORKER_EXECUTE_MAP, WORKER_EXECUTE_REDUCE,
    FIELD_JOB_ID, FIELD_WORKER_PORT, FIELD_WORKER_ID,
    FIELD_INPUT_PATH, FIELD_OUTPUT_PATH, FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_LINES, FIELD_REDUCE_RESULT, FIELD_PARTITION_ID,
    FIELD_MAP_WORKERS, FIELD_NUM_REDUCERS,
    FIELD_STATUS, FIELD_ERROR,
    STATUS_PENDING, STATUS_MAP_RUNNING, STATUS_SHUFFLING,
    STATUS_REDUCE_RUNNING, STATUS_COMPLETED, STATUS_FAILED,
    OUTPUT_DELIMITER, OUTPUT_LINE_END,
)
from .network import post_json, make_url


class Master:
    def __init__(self, port: int = 5000):
        self.port = port
        self.workers: List[Dict[str, Any]] = []
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def run(self):
        app = self._create_app()
        print(f"[Master] 启动于 http://0.0.0.0:{self.port}")
        app.run(host="0.0.0.0", port=self.port, threaded=True)

    def _create_app(self):
        from flask import Flask
        app = Flask(__name__)

        @app.route(MASTER_REGISTER, methods=["POST"])
        def register():
            return self._handle_register(request)

        @app.route(MASTER_SUBMIT_JOB, methods=["POST"])
        def submit_job():
            return self._handle_submit_job(request)

        @app.route(MASTER_MAP_DONE, methods=["POST"])
        def map_done():
            return self._handle_map_done(request)

        @app.route(MASTER_REDUCE_DONE, methods=["POST"])
        def reduce_done():
            return self._handle_reduce_done(request)

        @app.route(MASTER_JOB_STATUS + "/<job_id>", methods=["GET"])
        def job_status(job_id):
            return self._handle_job_status(job_id)

        return app

    def _handle_register(self, req):
        data = req.get_json()
        worker_port = data.get(FIELD_WORKER_PORT)
        if worker_port is None:
            return jsonify({FIELD_ERROR: "缺少 worker_port"}), 400

        worker_id = str(uuid.uuid4())[:8]
        worker_info = {
            FIELD_WORKER_ID: worker_id,
            "host": req.remote_addr,
            "port": worker_port,
        }

        with self._lock:
            for w in self.workers:
                if w["host"] == worker_info["host"] and w["port"] == worker_info["port"]:
                    worker_id = w[FIELD_WORKER_ID]
                    break
            else:
                self.workers.append(worker_info)

        print(f"[Master] Worker 注册成功: {worker_info['host']}:{worker_port} (id={worker_id})")
        return jsonify({FIELD_WORKER_ID: worker_id, "workers_count": len(self.workers)})

    def _handle_submit_job(self, req):
        data = req.get_json()
        input_path = data.get(FIELD_INPUT_PATH)
        output_path = data.get(FIELD_OUTPUT_PATH)
        mapper_pkl_b64 = data.get(FIELD_MAPPER_PKL)
        reducer_pkl_b64 = data.get(FIELD_REDUCER_PKL)

        if not all([input_path, output_path, mapper_pkl_b64, reducer_pkl_b64]):
            return jsonify({FIELD_ERROR: "缺少必要参数"}), 400

        job_id = str(uuid.uuid4())[:8]
        job = {
            FIELD_JOB_ID: job_id,
            FIELD_STATUS: STATUS_PENDING,
            FIELD_INPUT_PATH: input_path,
            FIELD_OUTPUT_PATH: output_path,
            FIELD_MAPPER_PKL: mapper_pkl_b64,
            FIELD_REDUCER_PKL: reducer_pkl_b64,
            "map_done_count": 0,
            "reduce_results": [],
            "reduce_done_count": 0,
            FIELD_ERROR: None,
        }
        with self._lock:
            self.jobs[job_id] = job

        threading.Thread(target=self._run_job, args=(job_id,), daemon=True).start()
        print(f"[Master] 作业已提交: {job_id}, 输入={input_path}, 输出={output_path}")
        return jsonify({FIELD_JOB_ID: job_id, FIELD_STATUS: STATUS_PENDING})

    def _handle_map_done(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID)
        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404
            job["map_done_count"] += 1
            count = job["map_done_count"]
        print(f"[Master] map_done: job={job_id}, count={count}/{len(self.workers)}")
        return jsonify({"ok": True})

    def _handle_reduce_done(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID)
        result = data.get(FIELD_REDUCE_RESULT, [])
        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404
            job["reduce_results"].extend(result)
            job["reduce_done_count"] += 1
            count = job["reduce_done_count"]
        print(f"[Master] reduce_done: job={job_id}, count={count}/{len(self.workers)}")
        return jsonify({"ok": True})

    def _handle_job_status(self, job_id: str):
        with self._lock:
            job = self.jobs.get(job_id)
        if job is None:
            return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404
        return jsonify({
            FIELD_JOB_ID: job[FIELD_JOB_ID],
            FIELD_STATUS: job[FIELD_STATUS],
            FIELD_ERROR: job.get(FIELD_ERROR),
        })

    # ================================================================
    # 作业执行
    # ================================================================

    def _run_job(self, job_id: str):
        try:
            with self._lock:
                job = self.jobs[job_id]
                workers = list(self.workers)

            if not workers:
                self._fail_job(job_id, "没有可用的 Worker")
                return

            input_path = job[FIELD_INPUT_PATH]
            output_path = job[FIELD_OUTPUT_PATH]
            mapper_pkl_b64 = job[FIELD_MAPPER_PKL]
            reducer_pkl_b64 = job[FIELD_REDUCER_PKL]
            n = len(workers)

            # 1. 读取输入
            lines = self._read_input(input_path)
            if lines is None:
                self._fail_job(job_id, f"无法读取输入文件: {input_path}")
                return
            print(f"[Master] 作业 {job_id}: {len(lines)} 行, {n} 个 Worker")

            # 2. Map
            self._set_status(job_id, STATUS_MAP_RUNNING)
            ok = self._dispatch_map(job_id, lines, workers, mapper_pkl_b64, n)
            if not ok:
                return
            print(f"[Master] 作业 {job_id}: Map 阶段完成")

            # map_workers 列表（host + port），给 reduce worker 拉取用
            map_workers = [{"host": w["host"], "port": w["port"]} for w in workers]

            # 3. Shuffle + Reduce（由 reduce worker 拉取 partition，本地 shuffle）
            self._set_status(job_id, STATUS_SHUFFLING)
            self._set_status(job_id, STATUS_REDUCE_RUNNING)
            ok = self._dispatch_reduce(job_id, n, workers, reducer_pkl_b64, map_workers)
            if not ok:
                return
            print(f"[Master] 作业 {job_id}: Reduce 阶段完成")

            # 4. 写入输出
            with self._lock:
                results = list(self.jobs[job_id]["reduce_results"])
            self._write_output(output_path, results)
            print(f"[Master] 作业 {job_id}: 结果已写入 {output_path}")
            self._set_status(job_id, STATUS_COMPLETED)
            print(f"[Master] 作业 {job_id}: 完成")

        except Exception as e:
            self._fail_job(job_id, f"作业执行异常: {str(e)}")

    def _read_input(self, path: str) -> Optional[List[str]]:
        try:
            with open(path, "r") as f:
                return [line.rstrip("\n").rstrip("\r") for line in f]
        except Exception as e:
            print(f"[Master] 读取输入文件失败: {e}")
            return None

    def _dispatch_map(self, job_id: str, lines: List[str], workers: List[Dict],
                      mapper_pkl_b64: str, num_reducers: int) -> bool:
        """分配 map 任务"""
        chunks = self._split_list(lines, len(workers))
        with self._lock:
            self.jobs[job_id]["map_done_count"] = 0

        threads = []
        send_errors = []

        def send_map(worker, chunk):
            try:
                url = make_url(worker["host"], worker["port"], WORKER_EXECUTE_MAP)
                resp = post_json(url, {
                    FIELD_JOB_ID: job_id,
                    FIELD_MAPPER_PKL: mapper_pkl_b64,
                    FIELD_LINES: chunk,
                    FIELD_NUM_REDUCERS: num_reducers,
                })
                if not resp.get("ok"):
                    send_errors.append(
                        f"Worker {worker['host']}:{worker['port']} map 失败: {resp.get(FIELD_ERROR, '')}")
            except Exception as e:
                send_errors.append(f"Worker {worker['host']}:{worker['port']} 通信失败: {e}")

        for worker, chunk in zip(workers, chunks):
            if not chunk:
                continue
            t = threading.Thread(target=send_map, args=(worker, chunk))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if send_errors:
            self._fail_job(job_id, "; ".join(send_errors))
            return False

        expected = sum(1 for c in chunks if c)
        self._wait_for_count(job_id, "map_done_count", expected, timeout=120)

        with self._lock:
            job = self.jobs[job_id]
            if job[FIELD_STATUS] == STATUS_FAILED:
                return False
            if job["map_done_count"] < expected:
                self._fail_job(job_id, f"Map 超时: {job['map_done_count']}/{expected}")
                return False
        return True

    def _dispatch_reduce(self, job_id: str, num_reducers: int, workers: List[Dict],
                         reducer_pkl_b64: str, map_workers: List[Dict]) -> bool:
        """分配 reduce 任务，每个 Worker 负责一个 partition"""
        with self._lock:
            self.jobs[job_id]["reduce_results"] = []
            self.jobs[job_id]["reduce_done_count"] = 0

        threads = []
        send_errors = []

        def send_reduce(worker, partition_id):
            try:
                url = make_url(worker["host"], worker["port"], WORKER_EXECUTE_REDUCE)
                resp = post_json(url, {
                    FIELD_JOB_ID: job_id,
                    FIELD_REDUCER_PKL: reducer_pkl_b64,
                    FIELD_PARTITION_ID: partition_id,
                    FIELD_MAP_WORKERS: map_workers,
                })
                if not resp.get("ok"):
                    send_errors.append(
                        f"Worker {worker['host']}:{worker['port']} reduce 失败: {resp.get(FIELD_ERROR, '')}")
            except Exception as e:
                send_errors.append(f"Worker {worker['host']}:{worker['port']} 通信失败: {e}")

        for i, worker in enumerate(workers[:num_reducers]):
            t = threading.Thread(target=send_reduce, args=(worker, i))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if send_errors:
            self._fail_job(job_id, "; ".join(send_errors))
            return False

        expected = min(len(workers), num_reducers)
        self._wait_for_count(job_id, "reduce_done_count", expected, timeout=120)

        with self._lock:
            job = self.jobs[job_id]
            if job[FIELD_STATUS] == STATUS_FAILED:
                return False
            if job["reduce_done_count"] < expected:
                self._fail_job(job_id, f"Reduce 超时: {job['reduce_done_count']}/{expected}")
                return False
        return True

    def _write_output(self, path: str, results: List[List]):
        with open(path, "w") as f:
            for pair in results:
                f.write(str(pair[0]))
                f.write(OUTPUT_DELIMITER)
                f.write(str(pair[1]))
                f.write(OUTPUT_LINE_END)

    def _wait_for_count(self, job_id: str, field: str, expected: int, timeout: float):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                job = self.jobs.get(job_id)
                if job is None or job[FIELD_STATUS] == STATUS_FAILED:
                    return
                if job.get(field, 0) >= expected:
                    return
            time.sleep(0.5)

    def _set_status(self, job_id: str, status: str):
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id][FIELD_STATUS] = status

    def _fail_job(self, job_id: str, error: str):
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id][FIELD_STATUS] = STATUS_FAILED
                self.jobs[job_id][FIELD_ERROR] = error
        print(f"[Master] 作业 {job_id}: 失败 - {error}")

    @staticmethod
    def _split_list(lst: List, n: int) -> List[List]:
        if n <= 0:
            return [lst]
        length = len(lst)
        chunk_size = max(1, length // n)
        remainder = length % n
        result = []
        idx = 0
        for i in range(n):
            extra = 1 if i < remainder else 0
            end = idx + chunk_size + extra
            result.append(lst[idx:min(end, length)])
            idx = end
        return [r for r in result if r]


def run_master(port: int = 5000):
    master = Master(port=port)
    master.run()