"""
Small sync response cache for development runs.

Stores GET responses on disk as JSON files keyed by a request fingerprint.
Designed for local use to avoid repeatedly hammering retailer sites during
iterative scraper development.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Optional


class CachedResponse:
    """Minimal response shim compatible with the fields the code reads."""

    def __init__(self, data: dict):
        self.status_code = int(data.get("status_code", 0))
        self.url = data.get("url")
        self.headers = data.get("headers", {})
        self.content = bytes.fromhex(data.get("content_hex", ""))
        self.text = data.get("text", "")


class ResponseCache:
    def __init__(self, cache_dir: str = ".dev_cache"):
        self._dir = Path(cache_dir)
        self._lock = threading.Lock()

    @staticmethod
    def fingerprint(url: str, method: str = "GET", body: bytes = b"") -> str:
        h = hashlib.sha256()
        h.update(method.encode("utf-8"))
        h.update(b"\x00")
        h.update(url.encode("utf-8"))
        h.update(b"\x00")
        h.update(body)
        return h.hexdigest()

    def _cache_path(self, fp: str) -> Path:
        return self._dir / f"{fp}.json"

    def get(self, fp: str) -> Optional[CachedResponse]:
        path = self._cache_path(fp)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return CachedResponse(raw)
        except Exception:
            return None

    def put(self, fp: str, resp) -> None:
        with self._lock:
            self._dir.mkdir(parents=True, exist_ok=True)
            path = self._cache_path(fp)
            tmp_path = path.with_suffix(".tmp")
            payload = {
                "status_code": getattr(resp, "status_code", 0),
                "url": getattr(resp, "url", ""),
                "headers": dict(getattr(resp, "headers", {}) or {}),
                "content_hex": (getattr(resp, "content", b"") or b"").hex(),
                "text": getattr(resp, "text", ""),
            }
            tmp_path.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp_path, path)

    def clear(self) -> None:
        if not self._dir.exists():
            return
        for p in self._dir.glob("*.json"):
            try:
                p.unlink()
            except Exception:
                continue
