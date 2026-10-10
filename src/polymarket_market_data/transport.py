"""Bounded read-only source transports; errors never expose credential URLs."""

from __future__ import annotations

import ipaddress
import socket
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .foundation import BudgetExceeded, json_loads

HOSTS = {
    "clob": {"clob.polymarket.com"},
    "pmxt": {"api.pmxt.dev"},
    "archive": {"archive.pmxt.dev", "r2.pmxt.dev", "r2v2.pmxt.dev"},
}
RPC_METHODS = {
    "eth_chainId",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getLogs",
    "eth_getTransactionReceipt",
    "eth_call",
    "eth_getCode",
    "web3_sha3",
}


class SourceError(ValueError):
    def __init__(self, status, message="source request failed"):
        self.status = status
        super().__init__(message)


def validate_url(source, url, resolve=True):
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        raise ValueError("invalid source URL") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.fragment
        or parsed.username
        or parsed.password
        or port not in (None, 443)
    ):
        raise ValueError("source requires standard HTTPS origin")
    if source != "rpc" and parsed.hostname not in HOSTS.get(source, set()):
        raise ValueError("unregistered provider host")
    if resolve:
        try:
            addresses = socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
        except OSError:
            raise SourceError("failed", "source name resolution failed") from None
        if not addresses or any(
            not ipaddress.ip_address(item[4][0]).is_global for item in addresses
        ):
            raise ValueError("non-public source address")
    return url


class Client:
    def __init__(
        self,
        source,
        limits,
        *,
        rpc_url=None,
        paid_ledger=None,
        account=None,
        period=None,
        allowance_id=None,
        transport=None,
        retries=3,
        sleep=time.sleep,
        storage_root=None,
    ):
        if source not in {*HOSTS, "rpc"}:
            raise ValueError("unknown source")
        if not isinstance(retries, int) or not 1 <= retries <= 4:
            raise ValueError("retry attempts must be 1..4")
        self.source, self.limits, self.rpc_url = source, limits, rpc_url
        self.storage_root = Path(storage_root) if storage_root is not None else None
        self.paid_ledger, self.account, self.period, self.allowance_id = (
            paid_ledger,
            account,
            period,
            allowance_id,
        )
        self.requests = self.downloaded = 0
        self.retries, self.sleep, self.testing = retries, sleep, transport is not None
        self.client = httpx.Client(
            transport=transport,
            timeout=httpx.Timeout(30, connect=10),
            follow_redirects=False,
            trust_env=False,
            headers={"Accept-Encoding": "identity"},
        )
        if source == "rpc":
            if not rpc_url:
                raise ValueError("explicit HTTPS RPC endpoint required")
            validate_url(source, rpc_url, resolve=not self.testing)

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _reserve(self):
        if self.requests >= self.limits.max_requests:
            raise BudgetExceeded("invocation request limit exhausted")
        if self.paid_ledger is not None:
            self.paid_ledger.reserve(self.account, self.period, self.allowance_id)
        self.requests += 1

    def reserve_request(self):
        self._reserve()

    def count_bytes(self, size):
        self._count(size)

    def _count(self, size):
        self.downloaded += size
        if self.downloaded > self.limits.max_download_bytes:
            raise BudgetExceeded("invocation downloaded-byte limit exhausted")

    def _validate(self, method, url, body):
        validate_url(self.source, url, resolve=not self.testing)
        if self.source == "rpc":
            if (
                url != self.rpc_url
                or method != "POST"
                or not isinstance(body, dict)
                or body.get("method") not in RPC_METHODS
            ):
                raise ValueError("RPC acquisition method not allowlisted")
        elif method not in {"GET", "HEAD"} and not (
            self.source == "pmxt"
            and method == "POST"
            and urlsplit(url).path
            in {"/api/polymarket/fetchOrderBook", "/api/polymarket/fetchTrades"}
        ):
            raise ValueError("read-only acquisition endpoint required")

    def _request(self, method, url, params=None, json=None, headers=None, destination=None):
        self._validate(method, url, json)
        for attempt in range(self.retries):
            self._reserve()
            try:
                with self.client.stream(
                    method, url, params=params, json=json, headers=headers
                ) as response:
                    status = response.status_code
                    if response.headers.get("content-encoding", "identity").lower() != "identity":
                        raise SourceError("invalid", "unexpected encoded HTTP body")
                    if status >= 400:
                        failed_size = 0
                        for chunk in response.iter_bytes():
                            self._count(len(chunk))
                            failed_size += len(chunk)
                            if failed_size > min(
                                self.limits.max_response_bytes, self.limits.max_temp_bytes
                            ):
                                raise BudgetExceeded("error response byte limit exhausted")
                    if 300 <= status < 400:
                        raise SourceError("invalid", "source redirect refused")
                    if status == 413:
                        raise SourceError("range_limit", "source range limit")
                    if status == 404:
                        raise SourceError("missing", "source object not found")
                    if status in (429, 500, 502, 503, 504):
                        if attempt + 1 == self.retries:
                            raise SourceError("failed", "transient source attempts exhausted")
                        retry = response.headers.get("retry-after", "")
                        delay = (
                            min(30, max(0, float(retry)))
                            if retry.isdigit()
                            else min(10, 2**attempt)
                        )
                    elif status >= 400:
                        raise SourceError("failed", "source HTTP request rejected")
                    else:
                        chunks, size = [], 0
                        retained = (
                            sum(
                                p.stat().st_size
                                for p in self.storage_root.rglob("*")
                                if p.is_file()
                            )
                            if self.storage_root
                            else 0
                        )
                        stream = open(destination, "wb") if destination else None
                        try:
                            for chunk in response.iter_bytes():
                                size += len(chunk)
                                self._count(len(chunk))
                                maximum = (
                                    self.limits.max_temp_bytes
                                    if destination
                                    else min(
                                        self.limits.max_response_bytes, self.limits.max_temp_bytes
                                    )
                                )
                                if destination and retained + size > self.limits.max_storage_bytes:
                                    raise BudgetExceeded("retained storage limit exhausted")
                                if size > maximum:
                                    raise BudgetExceeded("source response/temp limit exhausted")
                                if stream:
                                    stream.write(chunk)
                                else:
                                    chunks.append(chunk)
                            if stream:
                                import os

                                stream.flush()
                                os.fsync(stream.fileno())
                            return (
                                (b"".join(chunks), dict(response.headers))
                                if not destination
                                else {
                                    "size": size,
                                    "etag": response.headers.get("etag"),
                                    "path": str(destination),
                                }
                            )
                        finally:
                            if stream:
                                stream.close()
            except httpx.HTTPError:
                if attempt + 1 == self.retries:
                    raise SourceError("failed", "source transport attempts exhausted") from None
                delay = min(10, 2**attempt)
            self.sleep(delay)
        raise SourceError("failed")

    def json(self, method, url, params=None, json=None, headers=None):
        raw, _ = self._request(method, url, params=params, json=json, headers=headers)
        try:
            return json_loads(raw)
        except (ValueError, UnicodeError):
            raise SourceError("invalid", "malformed source JSON") from None

    def get_json(self, url, params=None, headers=None):
        return self.json("GET", url, params=params, headers=headers)

    def post_json(self, url, body, headers=None):
        return self.json("POST", url, json=body, headers=headers)

    def get_bytes(self, url, params=None):
        return self._request("GET", url, params=params)[0]

    def download(self, url, destination):
        destination = Path(destination)
        if destination.is_symlink() or destination.parent.is_symlink():
            raise ValueError("symlink download destination")
        destination.parent.mkdir(parents=True, exist_ok=True)
        return self._request("GET", url, destination=destination)
