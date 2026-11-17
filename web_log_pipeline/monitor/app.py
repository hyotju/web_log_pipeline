"""pipeline_output/logs 를 읽어 브라우저에 보여주는 모니터 (Python 표준 라이브러리만 사용)."""

import json
import os
import signal
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/workspace/pipeline_output"))
STATIC_DIR = Path(__file__).parent / "static"
FAULT_FLAG = OUTPUT_DIR / "control" / "fault_mode"
PORT = 8000
PUBLIC_PORT = os.getenv("PUBLIC_PORT", "8000")  # 브라우저에서 접속할 포트 (.env의 MONITOR_PORT)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # consumer가 쓰는 중인 마지막 줄은 건너뜀
    return rows


def parse_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def build_summary() -> dict:
    logs_dir = OUTPUT_DIR / "logs"
    ok_events = read_jsonl(logs_dir / "ok.jsonl")
    error_events = read_jsonl(logs_dir / "error.jsonl")
    events = ok_events + error_events
    for e in events:
        e["_ts"] = parse_ts(e.get("timestamp"))
    events = [e for e in events if e["_ts"] is not None]
    events.sort(key=lambda e: e["_ts"])

    now = datetime.now(timezone.utc)
    last_minute = [e for e in events if now - e["_ts"] <= timedelta(seconds=60)]
    last_minute_errors = sum(1 for e in last_minute if e.get("classification") == "error")

    per_path: dict[str, dict] = {}
    for e in events:
        p = per_path.setdefault(e["path"], {"path": e["path"], "total": 0, "error": 0, "latency_sum": 0})
        p["total"] += 1
        p["error"] += e.get("classification") == "error"
        p["latency_sum"] += e.get("latency_ms", 0)
    endpoints = [
        {
            "path": p["path"],
            "total": p["total"],
            "error": p["error"],
            "error_rate": p["error"] / p["total"],
            "avg_latency_ms": round(p["latency_sum"] / p["total"]),
        }
        for p in sorted(per_path.values(), key=lambda p: -p["total"])
    ]

    # 알림: 최근 1분 동안 요청 5건 이상인 경로 중 에러율 30% 이상인 곳
    minute_by_path: dict[str, list[int]] = {}
    for e in last_minute:
        c = minute_by_path.setdefault(e["path"], [0, 0])
        c[0] += 1
        c[1] += e.get("classification") == "error"
    alerts = [
        {"path": path, "total": total, "error": err, "error_rate": err / total}
        for path, (total, err) in minute_by_path.items()
        if total >= 5 and err / total >= 0.3
    ]

    # 이벤트 발생 → consumer 저장까지 걸린 시간 (최근 50건 평균)
    delays = []
    for e in events[-50:]:
        consumed = parse_ts(e.get("consumed_at"))
        if consumed:
            delays.append((consumed - e["_ts"]).total_seconds() * 1000)

    # 1분 전부터 10초 단위 에러율 (차트용)
    buckets = []
    for i in range(6):
        start = now - timedelta(seconds=60 - i * 10)
        end = start + timedelta(seconds=10)
        bucket = [e for e in events if start < e["_ts"] <= end]
        errors = sum(1 for e in bucket if e.get("classification") == "error")
        buckets.append({"label": f"-{60 - i * 10}s", "total": len(bucket), "error": errors})

    recent = [{k: v for k, v in e.items() if k != "_ts"} for e in events[-20:]][::-1]

    return {
        "total": len(events),
        "ok": len(ok_events),
        "error": len(error_events),
        "error_rate": len(error_events) / len(events) if events else 0,
        "last_minute_total": len(last_minute),
        "last_minute_error_rate": last_minute_errors / len(last_minute) if last_minute else 0,
        "avg_delay_ms": round(sum(delays) / len(delays)) if delays else None,
        "last_event_at": events[-1]["timestamp"] if events else None,
        "fault_mode": FAULT_FLAG.exists(),
        "alerts": alerts,
        "endpoints": endpoints,
        "buckets": buckets,
        "recent": recent,
    }


def toggle_fault() -> bool:
    if FAULT_FLAG.exists():
        FAULT_FLAG.unlink()
        return False
    FAULT_FLAG.parent.mkdir(parents=True, exist_ok=True)
    FAULT_FLAG.touch()
    return True


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data: dict) -> None:
        self._send(200, json.dumps(data, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self._send(200, (STATIC_DIR / "index.html").read_bytes(), "text/html; charset=utf-8")
        elif self.path == "/api/summary":
            self._json(build_summary())
        else:
            self._send(404, b"not found", "text/plain")

    def do_POST(self) -> None:
        if self.path == "/api/fault":
            self._json({"fault_mode": toggle_fault()})
        else:
            self._send(404, b"not found", "text/plain")

    def log_message(self, format, *args) -> None:
        pass  # 2초마다 오는 요청 로그는 숨김


def _raise_interrupt(signum, frame):
    raise KeyboardInterrupt


def main() -> None:
    signal.signal(signal.SIGTERM, _raise_interrupt)
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[monitor] 브라우저에서 http://localhost:{PUBLIC_PORT} 접속", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    print("[monitor] 종료", flush=True)


if __name__ == "__main__":
    main()
