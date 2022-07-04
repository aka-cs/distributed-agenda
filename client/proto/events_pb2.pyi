"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import google.protobuf.timestamp_pb2
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class Event(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    DESCRIPTION_FIELD_NUMBER: builtins.int
    GROUPID_FIELD_NUMBER: builtins.int
    DRAFT_FIELD_NUMBER: builtins.int
    id: builtins.int
    @property
    def start(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def end(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    name: typing.Text
    description: typing.Text
    groupId: builtins.int
    draft: builtins.bool
    def __init__(self,
        *,
        id: builtins.int = ...,
        start: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        end: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        name: typing.Text = ...,
        description: typing.Text = ...,
        groupId: builtins.int = ...,
        draft: builtins.bool = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["end",b"end","start",b"start"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["description",b"description","draft",b"draft","end",b"end","groupId",b"groupId","id",b"id","name",b"name","start",b"start"]) -> None: ...
global___Event = Event

class Confirmations(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class ConfirmationsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        value: builtins.bool
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: builtins.bool = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    CONFIRMATIONS_FIELD_NUMBER: builtins.int
    @property
    def Confirmations(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, builtins.bool]: ...
    def __init__(self,
        *,
        Confirmations: typing.Optional[typing.Mapping[typing.Text, builtins.bool]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["Confirmations",b"Confirmations"]) -> None: ...
global___Confirmations = Confirmations

class GetEventRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    id: builtins.int
    def __init__(self,
        *,
        id: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id",b"id"]) -> None: ...
global___GetEventRequest = GetEventRequest

class CreateEventRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    EVENT_FIELD_NUMBER: builtins.int
    @property
    def event(self) -> global___Event: ...
    def __init__(self,
        *,
        event: typing.Optional[global___Event] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["event",b"event"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["event",b"event"]) -> None: ...
global___CreateEventRequest = CreateEventRequest

class DeleteEventRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    id: builtins.int
    def __init__(self,
        *,
        id: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id",b"id"]) -> None: ...
global___DeleteEventRequest = DeleteEventRequest

class GetEventResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    EVENT_FIELD_NUMBER: builtins.int
    @property
    def event(self) -> global___Event: ...
    def __init__(self,
        *,
        event: typing.Optional[global___Event] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["event",b"event"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["event",b"event"]) -> None: ...
global___GetEventResponse = GetEventResponse

class CreateEventResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UNAVAILABLE_FIELD_NUMBER: builtins.int
    @property
    def unavailable(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        unavailable: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["unavailable",b"unavailable"]) -> None: ...
global___CreateEventResponse = CreateEventResponse

class DeleteEventResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___DeleteEventResponse = DeleteEventResponse

class ConfirmEventRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    EVENTID_FIELD_NUMBER: builtins.int
    eventId: builtins.int
    def __init__(self,
        *,
        eventId: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["eventId",b"eventId"]) -> None: ...
global___ConfirmEventRequest = ConfirmEventRequest

class ConfirmEventResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___ConfirmEventResponse = ConfirmEventResponse

class RejectEventRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    EVENTID_FIELD_NUMBER: builtins.int
    eventId: builtins.int
    def __init__(self,
        *,
        eventId: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["eventId",b"eventId"]) -> None: ...
global___RejectEventRequest = RejectEventRequest

class RejectEventResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___RejectEventResponse = RejectEventResponse
