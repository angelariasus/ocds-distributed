from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Outcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTCOME_UNSPECIFIED: _ClassVar[Outcome]
    OUTCOME_OK: _ClassVar[Outcome]
    OUTCOME_THROTTLED: _ClassVar[Outcome]
    OUTCOME_ERROR: _ClassVar[Outcome]
OUTCOME_UNSPECIFIED: Outcome
OUTCOME_OK: Outcome
OUTCOME_THROTTLED: Outcome
OUTCOME_ERROR: Outcome

class AcquireRequest(_message.Message):
    __slots__ = ("resource", "client_id")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    resource: str
    client_id: str
    def __init__(self, resource: _Optional[str] = ..., client_id: _Optional[str] = ...) -> None: ...

class AcquireResponse(_message.Message):
    __slots__ = ("waited_ms", "rate")
    WAITED_MS_FIELD_NUMBER: _ClassVar[int]
    RATE_FIELD_NUMBER: _ClassVar[int]
    waited_ms: int
    rate: float
    def __init__(self, waited_ms: _Optional[int] = ..., rate: _Optional[float] = ...) -> None: ...

class ReportRequest(_message.Message):
    __slots__ = ("resource", "client_id", "latency_ms", "outcome")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    resource: str
    client_id: str
    latency_ms: int
    outcome: Outcome
    def __init__(self, resource: _Optional[str] = ..., client_id: _Optional[str] = ..., latency_ms: _Optional[int] = ..., outcome: _Optional[_Union[Outcome, str]] = ...) -> None: ...

class ReportResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
