from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class LeadTimeRequest(_message.Message):
    __slots__ = ("monto_referencial", "metodo", "categoria", "departamento", "es_essalud")
    MONTO_REFERENCIAL_FIELD_NUMBER: _ClassVar[int]
    METODO_FIELD_NUMBER: _ClassVar[int]
    CATEGORIA_FIELD_NUMBER: _ClassVar[int]
    DEPARTAMENTO_FIELD_NUMBER: _ClassVar[int]
    ES_ESSALUD_FIELD_NUMBER: _ClassVar[int]
    monto_referencial: float
    metodo: str
    categoria: str
    departamento: str
    es_essalud: bool
    def __init__(self, monto_referencial: _Optional[float] = ..., metodo: _Optional[str] = ..., categoria: _Optional[str] = ..., departamento: _Optional[str] = ..., es_essalud: bool = ...) -> None: ...

class LeadTimeResponse(_message.Message):
    __slots__ = ("lead_time_dias", "model_version", "served_by")
    LEAD_TIME_DIAS_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SERVED_BY_FIELD_NUMBER: _ClassVar[int]
    lead_time_dias: float
    model_version: str
    served_by: str
    def __init__(self, lead_time_dias: _Optional[float] = ..., model_version: _Optional[str] = ..., served_by: _Optional[str] = ...) -> None: ...

class ModelInfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ModelInfoResponse(_message.Message):
    __slots__ = ("version", "trained_at", "training_rows", "mae_dias", "served_by")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TRAINED_AT_FIELD_NUMBER: _ClassVar[int]
    TRAINING_ROWS_FIELD_NUMBER: _ClassVar[int]
    MAE_DIAS_FIELD_NUMBER: _ClassVar[int]
    SERVED_BY_FIELD_NUMBER: _ClassVar[int]
    version: str
    trained_at: str
    training_rows: int
    mae_dias: float
    served_by: str
    def __init__(self, version: _Optional[str] = ..., trained_at: _Optional[str] = ..., training_rows: _Optional[int] = ..., mae_dias: _Optional[float] = ..., served_by: _Optional[str] = ...) -> None: ...
