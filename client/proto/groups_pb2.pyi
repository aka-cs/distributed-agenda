"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import proto.users_pb2
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _UserLevel:
    ValueType = typing.NewType('ValueType', builtins.int)
    V: typing_extensions.TypeAlias = ValueType
class _UserLevelEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_UserLevel.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    USER: _UserLevel.ValueType  # 0
    ADMIN: _UserLevel.ValueType  # 1
class UserLevel(_UserLevel, metaclass=_UserLevelEnumTypeWrapper):
    pass

USER: UserLevel.ValueType  # 0
ADMIN: UserLevel.ValueType  # 1
global___UserLevel = UserLevel


class Group(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    DESCRIPTION_FIELD_NUMBER: builtins.int
    id: builtins.int
    name: typing.Text
    description: typing.Text
    def __init__(self,
        *,
        id: builtins.int = ...,
        name: typing.Text = ...,
        description: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["description",b"description","id",b"id","name",b"name"]) -> None: ...
global___Group = Group

class UserList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    USERS_FIELD_NUMBER: builtins.int
    @property
    def Users(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        Users: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["Users",b"Users"]) -> None: ...
global___UserList = UserList

class GroupMembers(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class MembersEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.int
        @property
        def value(self) -> global___UserList: ...
        def __init__(self,
            *,
            key: builtins.int = ...,
            value: typing.Optional[global___UserList] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    MEMBERS_FIELD_NUMBER: builtins.int
    @property
    def members(self) -> google.protobuf.internal.containers.MessageMap[builtins.int, global___UserList]: ...
    def __init__(self,
        *,
        members: typing.Optional[typing.Mapping[builtins.int, global___UserList]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["members",b"members"]) -> None: ...
global___GroupMembers = GroupMembers

class GetGroupRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    id: builtins.int
    def __init__(self,
        *,
        id: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id",b"id"]) -> None: ...
global___GetGroupRequest = GetGroupRequest

class CreateGroupRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    GROUP_FIELD_NUMBER: builtins.int
    ADMINS_FIELD_NUMBER: builtins.int
    USERS_FIELD_NUMBER: builtins.int
    HIERARCHY_FIELD_NUMBER: builtins.int
    @property
    def group(self) -> global___Group: ...
    @property
    def admins(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    @property
    def users(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    hierarchy: builtins.bool
    def __init__(self,
        *,
        group: typing.Optional[global___Group] = ...,
        admins: typing.Optional[typing.Iterable[typing.Text]] = ...,
        users: typing.Optional[typing.Iterable[typing.Text]] = ...,
        hierarchy: builtins.bool = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["group",b"group"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["admins",b"admins","group",b"group","hierarchy",b"hierarchy","users",b"users"]) -> None: ...
global___CreateGroupRequest = CreateGroupRequest

class EditGroupRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    GROUP_FIELD_NUMBER: builtins.int
    @property
    def group(self) -> global___Group: ...
    def __init__(self,
        *,
        group: typing.Optional[global___Group] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["group",b"group"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["group",b"group"]) -> None: ...
global___EditGroupRequest = EditGroupRequest

class DeleteGroupRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    id: builtins.int
    def __init__(self,
        *,
        id: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id",b"id"]) -> None: ...
global___DeleteGroupRequest = DeleteGroupRequest

class GetGroupUsersRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    GROUPID_FIELD_NUMBER: builtins.int
    groupID: builtins.int
    def __init__(self,
        *,
        groupID: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["groupID",b"groupID"]) -> None: ...
global___GetGroupUsersRequest = GetGroupUsersRequest

class AddUserRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    USERID_FIELD_NUMBER: builtins.int
    GROUPID_FIELD_NUMBER: builtins.int
    LEVEL_FIELD_NUMBER: builtins.int
    userID: typing.Text
    groupID: builtins.int
    level: global___UserLevel.ValueType
    def __init__(self,
        *,
        userID: typing.Text = ...,
        groupID: builtins.int = ...,
        level: global___UserLevel.ValueType = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["groupID",b"groupID","level",b"level","userID",b"userID"]) -> None: ...
global___AddUserRequest = AddUserRequest

class RemoveUserRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    USERID_FIELD_NUMBER: builtins.int
    GROUPID_FIELD_NUMBER: builtins.int
    LEVEL_FIELD_NUMBER: builtins.int
    userID: typing.Text
    groupID: builtins.int
    level: global___UserLevel.ValueType
    def __init__(self,
        *,
        userID: typing.Text = ...,
        groupID: builtins.int = ...,
        level: global___UserLevel.ValueType = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["groupID",b"groupID","level",b"level","userID",b"userID"]) -> None: ...
global___RemoveUserRequest = RemoveUserRequest

class GetGroupResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    GROUP_FIELD_NUMBER: builtins.int
    @property
    def group(self) -> global___Group: ...
    def __init__(self,
        *,
        group: typing.Optional[global___Group] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["group",b"group"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["group",b"group"]) -> None: ...
global___GetGroupResponse = GetGroupResponse

class CreateGroupResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___CreateGroupResponse = CreateGroupResponse

class EditGroupResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___EditGroupResponse = EditGroupResponse

class DeleteGroupResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___DeleteGroupResponse = DeleteGroupResponse

class GetGroupUsersResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    USER_FIELD_NUMBER: builtins.int
    LEVEL_FIELD_NUMBER: builtins.int
    @property
    def user(self) -> proto.users_pb2.User: ...
    level: global___UserLevel.ValueType
    def __init__(self,
        *,
        user: typing.Optional[proto.users_pb2.User] = ...,
        level: global___UserLevel.ValueType = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["user",b"user"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["level",b"level","user",b"user"]) -> None: ...
global___GetGroupUsersResponse = GetGroupUsersResponse

class AddUserResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___AddUserResponse = AddUserResponse

class RemoveUserResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___RemoveUserResponse = RemoveUserResponse