import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Task:
    task_id: str
    source: str
    year: int
    month: int
    url: str
    sha_url: str
    published_at: str
    planned_at: str

    @classmethod
    def from_json(cls, raw: bytes) -> "Task":
        data = json.loads(raw)
        return cls(**{field: data[field] for field in cls.__dataclass_fields__})

    @property
    def object_key(self) -> str:
        return f"ocds/source={self.source}/year={self.year:04d}/month={self.month:02d}/{self.task_id}.json.zip"


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

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()
