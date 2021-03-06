"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import proto.events_pb2
import proto.groups_pb2
import proto.users_pb2
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _Action:
    ValueType = typing.NewType('ValueType', builtins.int)
    V: typing_extensions.TypeAlias = ValueType
class _ActionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_Action.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    CREATE: _Action.ValueType  # 0
    UPDATE: _Action.ValueType  # 1
    DELETE: _Action.ValueType  # 2
    JOINED: _Action.ValueType  # 3
    LEFT: _Action.ValueType  # 4
    CONFIRM: _Action.ValueType  # 5
    REJECT: _Action.ValueType  # 6
class Action(_Action, metaclass=_ActionEnumTypeWrapper):
    pass

CREATE: Action.ValueType  # 0
UPDATE: Action.ValueType  # 1
DELETE: Action.ValueType  # 2
JOINED: Action.ValueType  # 3
LEFT: Action.ValueType  # 4
CONFIRM: Action.ValueType  # 5
REJECT: Action.ValueType  # 6
global___Action = Action


class HistoryEntry(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ACTION_FIELD_NUMBER: builtins.int
    USER_FIELD_NUMBER: builtins.int
    GROUP_FIELD_NUMBER: builtins.int
    EVENT_FIELD_NUMBER: builtins.int
    action: global___Action.ValueType
    @property
    def user(self) -> proto.users_pb2.User: ...
    @property
    def group(self) -> proto.groups_pb2.Group: ...
    @property
    def event(self) -> proto.events_pb2.Event: ...
    def __init__(self,
        *,
        action: global___Action.ValueType = ...,
        user: typing.Optional[proto.users_pb2.User] = ...,
        group: typing.Optional[proto.groups_pb2.Group] = ...,
        event: typing.Optional[proto.events_pb2.Event] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["event",b"event","group",b"group","target",b"target","user",b"user"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["action",b"action","event",b"event","group",b"group","target",b"target","user",b"user"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["target",b"target"]) -> typing.Optional[typing_extensions.Literal["user","group","event"]]: ...
global___HistoryEntry = HistoryEntry

class History(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ENTRIES_FIELD_NUMBER: builtins.int
    @property
    def entries(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___HistoryEntry]: ...
    def __init__(self,
        *,
        entries: typing.Optional[typing.Iterable[global___HistoryEntry]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["entries",b"entries"]) -> None: ...
global___History = History

class AddHistoryEntryRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ENTRY_FIELD_NUMBER: builtins.int
    USERS_FIELD_NUMBER: builtins.int
    @property
    def entry(self) -> global___HistoryEntry: ...
    @property
    def users(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        entry: typing.Optional[global___HistoryEntry] = ...,
        users: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["entry",b"entry"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["entry",b"entry","users",b"users"]) -> None: ...
global___AddHistoryEntryRequest = AddHistoryEntryRequest

class AddHistoryEntryResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___AddHistoryEntryResponse = AddHistoryEntryResponse

class GetFullHistoryRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    USERNAME_FIELD_NUMBER: builtins.int
    username: typing.Text
    def __init__(self,
        *,
        username: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["username",b"username"]) -> None: ...
global___GetFullHistoryRequest = GetFullHistoryRequest

class GetFullHistoryResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ENTRY_FIELD_NUMBER: builtins.int
    @property
    def entry(self) -> global___HistoryEntry: ...
    def __init__(self,
        *,
        entry: typing.Optional[global___HistoryEntry] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["entry",b"entry"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["entry",b"entry"]) -> None: ...
global___GetFullHistoryResponse = GetFullHistoryResponse

class GetHistoryFromOffsetRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    USERNAME_FIELD_NUMBER: builtins.int
    OFFSET_FIELD_NUMBER: builtins.int
    username: typing.Text
    offset: builtins.int
    def __init__(self,
        *,
        username: typing.Text = ...,
        offset: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["offset",b"offset","username",b"username"]) -> None: ...
global___GetHistoryFromOffsetRequest = GetHistoryFromOffsetRequest

class GetHistoryFromOffsetResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ENTRY_FIELD_NUMBER: builtins.int
    @property
    def entry(self) -> global___HistoryEntry: ...
    def __init__(self,
        *,
        entry: typing.Optional[global___HistoryEntry] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["entry",b"entry"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["entry",b"entry"]) -> None: ...
global___GetHistoryFromOffsetResponse = GetHistoryFromOffsetResponse
