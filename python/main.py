"""
MapReduce 分布式框架统一入口
用法:
    python main.py master [--port 5000]
    python main.py worker --master localhost:5000 --port 5001
    python main.py client --master localhost:5000 --input ../input.txt --output ../output.txt
"""

import argparse
import os
import sys


def _cmd_master(args):
    """启动 Master 节点"""
    from distributed.master import run_master
    port = args.port or 5000
    run_master(port=port)


def _cmd_worker(args):
    """启动 Worker 节点"""
    from distributed.worker import run_worker
    master = args.master
    if ':' in master:
        host, port_str = master.split(':', 1)
        master_host = host
        master_port = int(port_str)
    else:
        master_host = master
        master_port = 5000

    worker_port = args.port or 5001
    run_worker(master_host=master_host, master_port=master_port, port=worker_port)


def _cmd_client(args):
    """启动 Client，提交作业"""
    from distributed.client import run_client
    master = args.master
    if ':' in master:
        host, port_str = master.split(':', 1)
        master_host = host
        master_port = int(port_str)
    else:
        master_host = master
        master_port = 5000

    run_client(
        master_host=master_host,
        master_port=master_port,
        input_path=args.input,
        output_path=args.output,
    )


def main():
    parser = argparse.ArgumentParser(
        description="MapReduce 分布式框架"
    )
    subparsers = parser.add_subparsers(dest="mode", help="运行模式")

    # ---- master ----
    p_master = subparsers.add_parser("master", help="启动 Master 节点 (JobTracker)")
    p_master.add_argument("--port", type=int, default=5000, help="Master 监听端口（默认 5000）")
    p_master.set_defaults(func=_cmd_master)

    # ---- worker ----
    p_worker = subparsers.add_parser("worker", help="启动 Worker 节点 (TaskTracker)")
    p_worker.add_argument("--master", required=True, help="Master 地址，如 localhost:5000")
    p_worker.add_argument("--port", type=int, default=5001, help="Worker 监听端口（默认 5001）")
    p_worker.set_defaults(func=_cmd_worker)

    # ---- client ----
    p_client = subparsers.add_parser("client", help="提交 MapReduce 作业")
    p_client.add_argument("--master", required=True, help="Master 地址，如 localhost:5000")
    p_client.add_argument("--input", required=True, help="输入文件路径")
    p_client.add_argument("--output", required=True, help="输出文件路径")
    p_client.set_defaults(func=_cmd_client)

    args = parser.parse_args()
    if not args.mode:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()