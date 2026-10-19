from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VectorClock(_message.Message):
    __slots__ = ("entries",)
    class EntriesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.ScalarMap[str, int]
    def __init__(self, entries: _Optional[_Mapping[str, int]] = ...) -> None: ...

class Version(_message.Message):
    __slots__ = ("value", "clock", "tombstone", "updated_at_ms")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CLOCK_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONE_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    clock: VectorClock
    tombstone: bool
    updated_at_ms: int
    def __init__(self, value: _Optional[bytes] = ..., clock: _Optional[_Union[VectorClock, _Mapping]] = ..., tombstone: bool = ..., updated_at_ms: _Optional[int] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("key", "r")
    KEY_FIELD_NUMBER: _ClassVar[int]
    R_FIELD_NUMBER: _ClassVar[int]
    key: str
    r: int
    def __init__(self, key: _Optional[str] = ..., r: _Optional[int] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("versions", "context", "replicas_answered")
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    REPLICAS_ANSWERED_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[Version]
    context: bytes
    replicas_answered: int
    def __init__(self, versions: _Optional[_Iterable[_Union[Version, _Mapping]]] = ..., context: _Optional[bytes] = ..., replicas_answered: _Optional[int] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("key", "value", "context", "w")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    W_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: bytes
    context: bytes
    w: int
    def __init__(self, key: _Optional[str] = ..., value: _Optional[bytes] = ..., context: _Optional[bytes] = ..., w: _Optional[int] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ("context", "replicas_acked", "hinted")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    REPLICAS_ACKED_FIELD_NUMBER: _ClassVar[int]
    HINTED_FIELD_NUMBER: _ClassVar[int]
    context: bytes
    replicas_acked: int
    hinted: int
    def __init__(self, context: _Optional[bytes] = ..., replicas_acked: _Optional[int] = ..., hinted: _Optional[int] = ...) -> None: ...

class DeleteRequest(_message.Message):
    __slots__ = ("key", "context", "w")
    KEY_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    W_FIELD_NUMBER: _ClassVar[int]
    key: str
    context: bytes
    w: int
    def __init__(self, key: _Optional[str] = ..., context: _Optional[bytes] = ..., w: _Optional[int] = ...) -> None: ...

class DeleteResponse(_message.Message):
    __slots__ = ("replicas_acked",)
    REPLICAS_ACKED_FIELD_NUMBER: _ClassVar[int]
    replicas_acked: int
    def __init__(self, replicas_acked: _Optional[int] = ...) -> None: ...

class StatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Member(_message.Message):
    __slots__ = ("id", "addr", "state", "incarnation", "heartbeat")
    ID_FIELD_NUMBER: _ClassVar[int]
    ADDR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    INCARNATION_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    id: str
    addr: str
    state: str
    incarnation: int
    heartbeat: int
    def __init__(self, id: _Optional[str] = ..., addr: _Optional[str] = ..., state: _Optional[str] = ..., incarnation: _Optional[int] = ..., heartbeat: _Optional[int] = ...) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("self", "members", "ring", "n", "r", "w", "keys", "pending_hints")
    SELF_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    RING_FIELD_NUMBER: _ClassVar[int]
    N_FIELD_NUMBER: _ClassVar[int]
    R_FIELD_NUMBER: _ClassVar[int]
    W_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    PENDING_HINTS_FIELD_NUMBER: _ClassVar[int]
    self: str
    members: _containers.RepeatedCompositeFieldContainer[Member]
    ring: _containers.RepeatedScalarFieldContainer[str]
    n: int
    r: int
    w: int
    keys: int
    pending_hints: int
    def __init__(self_, self: _Optional[str] = ..., members: _Optional[_Iterable[_Union[Member, _Mapping]]] = ..., ring: _Optional[_Iterable[str]] = ..., n: _Optional[int] = ..., r: _Optional[int] = ..., w: _Optional[int] = ..., keys: _Optional[int] = ..., pending_hints: _Optional[int] = ...) -> None: ...

class ReplicaGetRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: str
    def __init__(self, key: _Optional[str] = ...) -> None: ...

class ReplicaGetResponse(_message.Message):
    __slots__ = ("versions",)
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[Version]
    def __init__(self, versions: _Optional[_Iterable[_Union[Version, _Mapping]]] = ...) -> None: ...

class ReplicaPutRequest(_message.Message):
    __slots__ = ("key", "version", "hint_for")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    HINT_FOR_FIELD_NUMBER: _ClassVar[int]
    key: str
    version: Version
    hint_for: str
    def __init__(self, key: _Optional[str] = ..., version: _Optional[_Union[Version, _Mapping]] = ..., hint_for: _Optional[str] = ...) -> None: ...

class ReplicaPutResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GossipMember(_message.Message):
    __slots__ = ("id", "addr", "state", "incarnation", "heartbeat")
    ID_FIELD_NUMBER: _ClassVar[int]
    ADDR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    INCARNATION_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    id: str
    addr: str
    state: str
    incarnation: int
    heartbeat: int
    def __init__(self, id: _Optional[str] = ..., addr: _Optional[str] = ..., state: _Optional[str] = ..., incarnation: _Optional[int] = ..., heartbeat: _Optional[int] = ...) -> None: ...

class PingRequest(_message.Message):
    __slots__ = ("members",)
    FROM_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    members: _containers.RepeatedCompositeFieldContainer[GossipMember]
    def __init__(self, members: _Optional[_Iterable[_Union[GossipMember, _Mapping]]] = ..., **kwargs) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("members",)
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    members: _containers.RepeatedCompositeFieldContainer[GossipMember]
    def __init__(self, members: _Optional[_Iterable[_Union[GossipMember, _Mapping]]] = ...) -> None: ...

class PingReqRequest(_message.Message):
    __slots__ = ("target_id", "target_addr")
    FROM_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ADDR_FIELD_NUMBER: _ClassVar[int]
    target_id: str
    target_addr: str
    def __init__(self, target_id: _Optional[str] = ..., target_addr: _Optional[str] = ..., **kwargs) -> None: ...

class PingReqResponse(_message.Message):
    __slots__ = ("reachable",)
    REACHABLE_FIELD_NUMBER: _ClassVar[int]
    reachable: bool
    def __init__(self, reachable: bool = ...) -> None: ...

class TreeLevelRequest(_message.Message):
    __slots__ = ("depth", "peer")
    DEPTH_FIELD_NUMBER: _ClassVar[int]
    PEER_FIELD_NUMBER: _ClassVar[int]
    depth: int
    peer: str
    def __init__(self, depth: _Optional[int] = ..., peer: _Optional[str] = ...) -> None: ...

class TreeLevelResponse(_message.Message):
    __slots__ = ("hashes",)
    HASHES_FIELD_NUMBER: _ClassVar[int]
    hashes: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, hashes: _Optional[_Iterable[bytes]] = ...) -> None: ...

class BucketKeysRequest(_message.Message):
    __slots__ = ("bucket", "peer")
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    PEER_FIELD_NUMBER: _ClassVar[int]
    bucket: int
    peer: str
    def __init__(self, bucket: _Optional[int] = ..., peer: _Optional[str] = ...) -> None: ...

class BucketKeysResponse(_message.Message):
    __slots__ = ("keys", "versions")
    KEYS_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedScalarFieldContainer[str]
    versions: _containers.RepeatedCompositeFieldContainer[Version]
    def __init__(self, keys: _Optional[_Iterable[str]] = ..., versions: _Optional[_Iterable[_Union[Version, _Mapping]]] = ...) -> None: ...
