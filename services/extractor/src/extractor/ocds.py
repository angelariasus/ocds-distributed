from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# The portal's WAF rejects generic user agents with 403.
USER_AGENT = "ocds-distributed/0.1 (+https://github.com/angelariasus/ocds-distributed)"
CHUNK_SIZE = 1 << 20


class OcdsHttp:
    def __init__(self, timeout: float = 120.0):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        retry = Retry(total=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        self.raw_session = requests.Session()
        self.raw_session.headers["User-Agent"] = USER_AGENT

    def fetch_sha(self, url: str) -> str:
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text.strip().lower()

    def download(self, url: str, dest: Path) -> int:
        size = 0
        with self.session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            with dest.open("wb") as fh:
                for chunk in resp.iter_content(CHUNK_SIZE):
                    fh.write(chunk)
                    size += len(chunk)
        return size

    def get_json(self, url: str) -> tuple[int, dict]:
        resp = self.raw_session.get(url, timeout=self.timeout)
        body = resp.json() if resp.status_code == 200 else {}
        return resp.status_code, body
