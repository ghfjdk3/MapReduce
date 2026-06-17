"""网络通信工具 — HTTP 连接池"""

import requests
import threading
from typing import Optional, Dict, Any
from .config import HTTP_TIMEOUT

_session_store = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_session_store, 'session'):
        _session_store.session = requests.Session()
    return _session_store.session


def post_json(url: str, data: Optional[Dict[str, Any]] = None,
              timeout: int = HTTP_TIMEOUT) -> Dict[str, Any]:
    ses = _get_session()
    try:
        resp = ses.post(url, json=data, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        raise NetworkError(f"无法连接到 {url}")
    except requests.exceptions.Timeout:
        raise NetworkError(f"请求 {url} 超时")
    except requests.exceptions.HTTPError as e:
        detail = ""
        try:
            detail = e.response.json().get('error', '')
        except Exception:
            pass
        raise NetworkError(f"HTTP {e.response.status_code}" + (f": {detail}" if detail else ""))
    except requests.exceptions.RequestException as e:
        raise NetworkError(f"请求异常: {e}")


def get_json(url: str, timeout: int = HTTP_TIMEOUT) -> Dict[str, Any]:
    ses = _get_session()
    try:
        resp = ses.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        raise NetworkError(f"无法连接到 {url}")
    except requests.exceptions.Timeout:
        raise NetworkError(f"请求 {url} 超时")
    except requests.exceptions.HTTPError as e:
        detail = ""
        try:
            detail = e.response.json().get('error', '')
        except Exception:
            pass
        raise NetworkError(f"HTTP {e.response.status_code}" + (f": {detail}" if detail else ""))
    except requests.exceptions.RequestException as e:
        raise NetworkError(f"请求异常: {e}")


def make_url(host: str, port: int, path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"http://{host}:{port}{path}"


class NetworkError(Exception):
    pass