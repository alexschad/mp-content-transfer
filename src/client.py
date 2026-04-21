from __future__ import annotations

from dataclasses import dataclass
import json
import random
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import EndpointConfig, RetryConfig
from .rate_limit import RateLimiter


class ApiError(RuntimeError):
    pass


@dataclass
class Response:
    status_code: int
    content: bytes
    headers: dict[str, str]

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)


@dataclass
class MPClient:
    endpoint: EndpointConfig
    retry: RetryConfig
    limiter: RateLimiter

    @classmethod
    def create(cls, endpoint: EndpointConfig, retry: RetryConfig) -> "MPClient":
        return cls(endpoint=endpoint, retry=retry, limiter=RateLimiter(endpoint.requests_per_second))

    def get(self, path: str, params: dict[str, Any] | None = None, ok_statuses: tuple[int, ...] = (200,)) -> Response:
        return self._request("GET", path, params=params, ok_statuses=ok_statuses)

    def put(self, path: str, json: dict[str, Any] | None = None, ok_statuses: tuple[int, ...] = (200,)) -> Response:
        return self._request("PUT", path, json=json, ok_statuses=ok_statuses)

    def post(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        ok_statuses: tuple[int, ...] = (200, 201),
    ) -> Response:
        return self._request("POST", path, json=json, data=data, headers=headers, ok_statuses=ok_statuses)

    def get_json(self, path: str, params: dict[str, Any] | None = None, ok_statuses: tuple[int, ...] = (200,)) -> Any:
        response = self.get(path, params=params, ok_statuses=ok_statuses)
        if response.status_code != 200:
            return None
        return response.json()

    def resource_exists(self, path: str) -> bool:
        response = self.get(path, ok_statuses=(200, 404))
        return response.status_code == 200

    def iter_collection(self, path: str, params: dict[str, Any] | None = None):
        params = dict(params or {})
        params.setdefault("page", 1)
        params.setdefault("rpp", 20)
        while True:
            data = self.get_json(path, params=params)
            for item in data.get("items", []):
                yield item
            next_fragment = data.get("next")
            if not next_fragment:
                break
            params = _merge_next_params(next_fragment, params)

    def download(self, url: str) -> bytes:
        return self._request("GET", url, absolute=True, ok_statuses=(200,)).content

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        data: bytes | None = None,
        headers: dict[str, str] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
        absolute: bool = False,
    ) -> Response:
        url = path if absolute else f"{self.endpoint.base_url}/{self.endpoint.instance_id}{path}"
        merged_headers = {"Authorization": f"bearer {self.endpoint.api_key}"}
        if headers:
            merged_headers.update(headers)
        attempt = 0
        while True:
            self.limiter.acquire()
            response = self._send(method=method, url=url, params=params, json_body=json, data=data, headers=merged_headers)
            if response.status_code in ok_statuses:
                return response
            if response.status_code not in {429, 500, 502, 503, 504} or attempt >= self.retry.retry_count:
                raise ApiError(f"{method} {url} failed with {response.status_code}: {response.text[:500]}")
            delay = min(self.retry.backoff_base_seconds * (2**attempt), self.retry.backoff_max_seconds)
            delay += random.uniform(0, self.retry.backoff_jitter_seconds)
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    pass
            time.sleep(delay)
            attempt += 1

    def _send(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json_body: dict[str, Any] | None,
        data: bytes | None,
        headers: dict[str, str],
    ) -> Response:
        if params:
            query = urlencode(params)
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{query}"
        request_data = data
        send_headers = dict(headers)
        if json_body is not None:
            request_data = json.dumps(json_body).encode("utf-8")
            send_headers.setdefault("Content-Type", "application/json")
        request = Request(url=url, data=request_data, headers=send_headers, method=method)
        try:
            with urlopen(request, timeout=60) as resp:
                return Response(
                    status_code=resp.getcode(),
                    content=resp.read(),
                    headers=dict(resp.headers.items()),
                )
        except HTTPError as exc:
            return Response(
                status_code=exc.code,
                content=exc.read(),
                headers=dict(exc.headers.items()),
            )
        except URLError as exc:
            raise ApiError(f"{method} {url} failed: {exc}") from exc


def _merge_next_params(next_fragment: str, current: dict[str, Any]) -> dict[str, Any]:
    new_params = dict(current)
    for pair in next_fragment.split("&"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        if value.isdigit():
            new_params[key] = int(value)
        else:
            new_params[key] = value
    return new_params
