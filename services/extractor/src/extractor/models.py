import json
from dataclasses import asdict, dataclass

BULK_FILE = "bulk_file"
RECORDS_RANGE = "records_range"


@dataclass(frozen=True)
class Task:
    task_id: str
    source: str
    planned_at: str
    kind: str = BULK_FILE
    year: int | None = None
    month: int | None = None
    url: str | None = None
    sha_url: str | None = None
    published_at: str | None = None
    prefix: str | None = None

    @classmethod
    def from_json(cls, raw: bytes) -> "Task":
        data = json.loads(raw)
        known = cls.__dataclass_fields__
        task = cls(**{key: value for key, value in data.items() if key in known})
        if task.kind == BULK_FILE and not (task.url and task.sha_url):
            raise ValueError("bulk_file task needs url and sha_url")
        if task.kind == RECORDS_RANGE and not task.prefix:
            raise ValueError("records_range task needs prefix")
        return task

    @property
    def object_key(self) -> str:
        return f"ocds/source={self.source}/year={self.year:04d}/month={self.month:02d}/{self.task_id}.json.zip"

    def page_key(self, first_tender_id: str) -> str:
        return f"ocds/api/source={self.source}/prefix={self.prefix}/{first_tender_id}.json"


@dataclass(frozen=True)
class Result:
    task_id: str
    status: str
    object_key: str
    sha256: str
    size_bytes: int
    attempts: int
    worker_id: str
    finished_at: str
    pages: int = 0
    records: int = 0

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()
