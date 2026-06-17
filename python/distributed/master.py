"""Master 节点 (JobTracker) — 双 Slot 管理 + 提前触发 Reduce"""

import os
import uuid
import threading
import time
from typing import List, Dict, Any, Optional

from flask import Flask, request, jsonify

from .protocol import (
    MASTER_REGISTER, MASTER_SUBMIT_JOB, MASTER_MAP_DONE, MASTER_REDUCE_DONE,
    MASTER_JOB_STATUS, WORKER_EXECUTE_MAP, WORKER_EXECUTE_REDUCE,
    WORKER_NOTIFY_MAP_READY,
    FIELD_JOB_ID, FIELD_WORKER_PORT, FIELD_WORKER_ID,
    FIELD_INPUT_PATH, FIELD_OUTPUT_PATH, FIELD_MAPPER_PKL, FIELD_REDUCER_PKL,
    FIELD_LINES, FIELD_REDUCE_RESULT, FIELD_PARTITION_ID,
    FIELD_MAP_WORKER_INFO, FIELD_NUM_REDUCERS, FIELD_TOTAL_MAP_TASKS,
    FIELD_SLOT_TYPE, FIELD_STATUS, FIELD_ERROR,
    STATUS_PENDING, STATUS_MAP_RUNNING, STATUS_SHUFFLING,
    STATUS_REDUCE_RUNNING, STATUS_COMPLETED, STATUS_FAILED,
    OUTPUT_DELIMITER, OUTPUT_LINE_END,
    SLOT_TYPE_MAP, SLOT_TYPE_REDUCE, MAP_PROGRESS_TRIGGER_RATIO,
)
from .network import post_json, make_url


class Master:
    def __init__(self, port: int = 5000):
        self.port = port
        self.map_slots: List[Dict[str, Any]] = []
        self.reduce_slots: List[Dict[str, Any]] = []
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
        slot_type = data.get(FIELD_SLOT_TYPE, SLOT_TYPE_MAP)
        if worker_port is None:
            return jsonify({FIELD_ERROR: "缺少 worker_port"}), 400

        worker_id = str(uuid.uuid4())[:8]
        worker_info = {
            FIELD_WORKER_ID: worker_id,
            "host": req.remote_addr,
            "port": worker_port,
            FIELD_SLOT_TYPE: slot_type,
        }

        with self._lock:
            if slot_type == SLOT_TYPE_MAP:
                slots = self.map_slots
            else:
                slots = self.reduce_slots

            for w in slots:
                if w["host"] == worker_info["host"] and w["port"] == worker_info["port"]:
                    worker_id = w[FIELD_WORKER_ID]
                    break
            else:
                slots.append(worker_info)

        print(f"[Master] 注册: {slot_type} slot {worker_info['host']}:{worker_port} (id={worker_id})")
        return jsonify({FIELD_WORKER_ID: worker_id, "map_count": len(self.map_slots),
                        "reduce_count": len(self.reduce_slots)})

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
            "reduce_dispatched": False,
            FIELD_ERROR: None,
        }
        with self._lock:
            self.jobs[job_id] = job

        threading.Thread(target=self._run_job, args=(job_id,), daemon=True).start()
        print(f"[Master] 作业已提交: {job_id}, 输入={input_path}")
        return jsonify({FIELD_JOB_ID: job_id, FIELD_STATUS: STATUS_PENDING})

    def _handle_map_done(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID)
        worker_id = data.get(FIELD_WORKER_ID, "")

        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404
            job["map_done_count"] += 1
            count = job["map_done_count"]
            total = job.get("_total_map_tasks", len(self.map_slots))

        worker_info = None
        for w in self.map_slots:
            if w[FIELD_WORKER_ID] == worker_id:
                worker_info = {"host": w["host"], "port": w["port"]}
                break

        print(f"[Master] map_done: job={job_id}, {count}/{total}")

        # 检查是否已触发 reduce（10% 阈值）
        dispatched = False
        with self._lock:
            if not job.get("reduce_dispatched") and count / max(total, 1) >= MAP_PROGRESS_TRIGGER_RATIO:
                job["reduce_dispatched"] = True
                dispatched = True

        if dispatched:
            self._dispatch_reduce_init(job_id, total)

        # 通知所有 reduce slots：这个 map worker 的数据已就绪
        if worker_info:
            self._notify_all_reduce(job_id, worker_info)

        return jsonify({"ok": True})

    def _handle_reduce_done(self, req):
        data = req.get_json()
        job_id = data.get(FIELD_JOB_ID)
        result = data.get(FIELD_REDUCE_RESULT, [])
        worker_id = data.get(FIELD_WORKER_ID, "")
        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return jsonify({FIELD_ERROR: f"作业不存在: {job_id}"}), 404
            job["reduce_results"].extend(result)
            job["reduce_done_count"] += 1
            count = job["reduce_done_count"]
        print(f"[Master] reduce_done: job={job_id}, worker={worker_id}, count={count}/{len(self.reduce_slots)}")
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
                map_slots = list(self.map_slots)
                reduce_slots = list(self.reduce_slots)

            if not map_slots or not reduce_slots:
                self._fail_job(job_id, "没有可用的 Slot")
                return

            input_path = job[FIELD_INPUT_PATH]
            output_path = job[FIELD_OUTPUT_PATH]
            mapper_pkl_b64 = job[FIELD_MAPPER_PKL]
            reducer_pkl_b64 = job[FIELD_REDUCER_PKL]
            num_reducers = len(reduce_slots)
            total_map_tasks = len(map_slots)

            # 保存总数供 map_done 使用
            with self._lock:
                self.jobs[job_id]["_total_map_tasks"] = total_map_tasks

            # 1. 读取输入
            lines = self._read_input(input_path)
            if lines is None:
                self._fail_job(job_id, f"无法读取: {input_path}")
                return
            print(f"[Master] 作业 {job_id}: {len(lines)} 行, {total_map_tasks} map + {num_reducers} reduce slots")

            # 2. Map 阶段
            self._set_status(job_id, STATUS_MAP_RUNNING)
            ok = self._dispatch_map(job_id, lines, map_slots, mapper_pkl_b64, num_reducers)
            if not ok:
                return

            # 等待 map 全部完成
            self._wait_for_count(job_id, "map_done_count", total_map_tasks, timeout=120)
            with self._lock:
                j = self.jobs[job_id]
                if j[FIELD_STATUS] == STATUS_FAILED:
                    return
                if j["map_done_count"] < total_map_tasks:
                    self._fail_job(job_id, f"Map 超时: {j['map_done_count']}/{total_map_tasks}")
                    return

            print(f"[Master] 作业 {job_id}: Map 全部完成")

            # 3. 如果没有提前触发，现在触发 reduce
            with self._lock:
                j = self.jobs[job_id]
                if not j.get("reduce_dispatched"):
                    j["reduce_dispatched"] = True
                    self._dispatch_reduce_init(job_id, total_map_tasks)
                    # 通知所有 map workers 已就绪（可能已经提前通知过）
                    for mw in map_slots:
                        self._notify_all_reduce(job_id, {"host": mw["host"], "port": mw["port"]})

            # 4. 等待 reduce 完成
            self._set_status(job_id, STATUS_SHUFFLING)
            self._set_status(job_id, STATUS_REDUCE_RUNNING)
            self._wait_for_count(job_id, "reduce_done_count", num_reducers, timeout=120)
            with self._lock:
                j = self.jobs[job_id]
                if j[FIELD_STATUS] == STATUS_FAILED:
                    return
                if j["reduce_done_count"] < num_reducers:
                    self._fail_job(job_id, f"Reduce 超时: {j['reduce_done_count']}/{num_reducers}")
                    return

            # 5. 输出
            print(f"[Master] 作业 {job_id}: Reduce 全部完成")
            with self._lock:
                results = list(self.jobs[job_id]["reduce_results"])
            self._write_output(output_path, results)
            print(f"[Master] 作业 {job_id}: 结果已写入 {output_path}")
            self._set_status(job_id, STATUS_COMPLETED)
            print(f"[Master] 作业 {job_id}: 完成")

        except Exception as e:
            self._fail_job(job_id, f"异常: {str(e)}")

    def _read_input(self, path: str) -> Optional[List[str]]:
        try:
            with open(path, "r") as f:
                return [line.rstrip("\n").rstrip("\r") for line in f]
        except Exception as e:
            print(f"[Master] 读取失败: {e}")
            return None

    def _dispatch_map(self, job_id: str, lines: List[str], map_slots: List[Dict],
                      mapper_pkl_b64: str, num_reducers: int) -> bool:
        chunks = self._split_list(lines, len(map_slots))
        with self._lock:
            self.jobs[job_id]["map_done_count"] = 0

        threads = []
        send_errors = []

        def send_map(slot, chunk):
            try:
                url = make_url(slot["host"], slot["port"], WORKER_EXECUTE_MAP)
                resp = post_json(url, {
                    FIELD_JOB_ID: job_id,
                    FIELD_MAPPER_PKL: mapper_pkl_b64,
                    FIELD_LINES: chunk,
                    FIELD_NUM_REDUCERS: num_reducers,
                })
                if not resp.get("ok"):
                    send_errors.append(f"Map {slot['host']}:{slot['port']} 失败: {resp.get(FIELD_ERROR)}")
            except Exception as e:
                send_errors.append(f"Map {slot['host']}:{slot['port']}: {e}")

        for slot, chunk in zip(map_slots, chunks):
            if not chunk:
                continue
            t = threading.Thread(target=send_map, args=(slot, chunk))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        if send_errors:
            self._fail_job(job_id, "; ".join(send_errors))
            return False
        return True

    def _dispatch_reduce_init(self, job_id: str, total_map_tasks: int):
        """首次 dispatch reduce 任务（10% map 完成时触发）"""
        with self._lock:
            job = self.jobs[job_id]
            reducer_pkl_b64 = job[FIELD_REDUCER_PKL]
            reduce_slots = list(self.reduce_slots)

        print(f"[Master] 作业 {job_id}: 提前触发 reduce ({len(reduce_slots)} tasks)")

        for i, slot in enumerate(reduce_slots):
            t = threading.Thread(target=self._send_reduce_init, args=(
                slot, job_id, reducer_pkl_b64, i, total_map_tasks))
            t.start()
            t.join()

    def _send_reduce_init(self, slot: Dict, job_id: str, reducer_pkl_b64: str,
                          partition_id: int, total_map_tasks: int):
        try:
            url = make_url(slot["host"], slot["port"], WORKER_EXECUTE_REDUCE)
            post_json(url, {
                FIELD_JOB_ID: job_id,
                FIELD_REDUCER_PKL: reducer_pkl_b64,
                FIELD_PARTITION_ID: partition_id,
                FIELD_TOTAL_MAP_TASKS: total_map_tasks,
            })
        except Exception as e:
            print(f"[Master] reduce init 失败 {slot['host']}:{slot['port']}: {e}")

    def _notify_all_reduce(self, job_id: str, map_worker_info: Dict):
        """通知所有 reduce slots：一个 map worker 数据已就绪"""
        with self._lock:
            reduce_slots = list(self.reduce_slots)

        for slot in reduce_slots:
            t = threading.Thread(target=self._send_notify, args=(slot, job_id, map_worker_info))
            t.start()
            t.join()

    def _send_notify(self, slot: Dict, job_id: str, map_worker_info: Dict):
        try:
            url = make_url(slot["host"], slot["port"], WORKER_NOTIFY_MAP_READY)
            post_json(url, {
                FIELD_JOB_ID: job_id,
                FIELD_MAP_WORKER_INFO: map_worker_info,
            })
        except Exception as e:
            print(f"[Master] notify 失败 {slot['host']}:{slot['port']}: {e}")

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
                j = self.jobs.get(job_id)
                if j is None or j[FIELD_STATUS] == STATUS_FAILED:
                    return
                if j.get(field, 0) >= expected:
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
    Master(port=port).run()