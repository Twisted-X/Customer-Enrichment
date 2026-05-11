"""
Thread-safe proxy rotation inspired by Scrapling's ProxyRotator.
"""
from __future__ import annotations

import os
from threading import Lock
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

ProxyType = Union[str, Dict[str, str]]

_GLOBAL_ROTATOR: Optional["ProxyRotator"] = None


def _split_credentials(proxy_url: str) -> Dict[str, str]:
    parsed = urlparse(proxy_url)
    out: Dict[str, str] = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        out["username"] = parsed.username
    if parsed.password:
        out["password"] = parsed.password
    return out


class ProxyRotator:
    __slots__ = ("_proxies", "_current_index", "_lock")

    def __init__(self, proxies: List[ProxyType]):
        if not proxies:
            raise ValueError("At least one proxy must be provided")
        self._proxies: List[ProxyType] = []
        for proxy in proxies:
            if not isinstance(proxy, (str, dict)):
                raise TypeError("Proxy must be str or dict")
            if isinstance(proxy, dict) and "server" not in proxy:
                raise ValueError("Proxy dict must contain 'server'")
            self._proxies.append(proxy)
        self._current_index = 0
        self._lock = Lock()

    def get_proxy(self) -> ProxyType:
        with self._lock:
            idx = self._current_index % len(self._proxies)
            proxy = self._proxies[idx]
            self._current_index = (idx + 1) % len(self._proxies)
            return proxy

    def get_for_curl_cffi(self) -> Optional[Dict[str, str]]:
        proxy = self.get_proxy()
        if isinstance(proxy, dict):
            server = proxy.get("server")
        else:
            server = proxy
        if not server:
            return None
        return {"http": server, "https": server}

    def get_for_playwright(self) -> Optional[Dict[str, str]]:
        proxy = self.get_proxy()
        if isinstance(proxy, dict):
            return dict(proxy)
        return _split_credentials(proxy)

    def __len__(self) -> int:
        return len(self._proxies)


def get_global_rotator() -> Optional[ProxyRotator]:
    global _GLOBAL_ROTATOR
    if _GLOBAL_ROTATOR is not None:
        return _GLOBAL_ROTATOR
    raw = os.getenv("PROXY_LIST", "").strip()
    if not raw:
        return None
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    if not proxies:
        return None
    _GLOBAL_ROTATOR = ProxyRotator(proxies)
    return _GLOBAL_ROTATOR
