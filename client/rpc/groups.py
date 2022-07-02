import logging

from proto.history_pb2 import Action
from rpc import services
from rpc.client import Channel
from rpc.history import get_history

import proto.groups_grpc
import proto.groups_pb2
from proto.groups_pb2 import UserLevel


async def get_all_groups():
    history = await get_history()

    groups = dict()
    owner = dict()

    # add group if group created, remove if group deleted
    for entry in history:
        if not entry.group:
            continue
        if entry.action == Action.CREATE:
            groups[entry.group.id] = entry.group
            owner[entry.group.id] = True
        if entry.action == Action.DELETE:
            del groups[entry.group.id]
        if entry.action == Action.JOINED:
            groups[entry.group.id] = entry.group
        if entry.action == Action.LEFT:
            del groups[entry.group.id]

    return groups, owner


async def create_group(users, hierarchy=False):

    new_group = proto.groups_pb2.Group()
    request = proto.groups_pb2.CreateGroupRequest(group=new_group, users=users, hierarchy=hierarchy)

    async with Channel(services.GROUP) as channel:
        stub = proto.groups_grpc.GroupServiceStub(channel)
        logging.info(f'Creating group with users {users} and hierarchy {hierarchy}')
        response = await stub.CreateGroup(request)
        logging.info(f'Group created with result {response.result}')

    return response


async def get_group_users(group_id):

    request = proto.groups_pb2.GetGroupUsersRequest(groupID=group_id)

    async with Channel(services.GROUP) as channel:
        stub = proto.groups_grpc.GroupServiceStub(channel)
        logging.info(f'Retrieving group users for group {group_id}')
        response = await stub.GetGroupUsers(request)
        logging.info(f'Group users retrieved: {response}')
        users = [r.user for r in response]
        admins = [UserLevel.ADMIN == r.level for r in response]

    return users, admins


# Adds a user to a group, by group ID and user username
async def add_user_to_group(group_id, user_name):
    request = proto.groups_pb2.AddUserRequest(groupID=group_id, userID=user_name)

    async with Channel(services.GROUP) as channel:
        stub = proto.groups_grpc.GroupServiceStub(channel)
        logging.info(f'Adding user {user_name} to group {group_id}')
        response = await stub.AddUser(request)
        logging.info(f'User {user_name} added to group {group_id}')

    return response


# Removes a user from a group, by group ID and user username
async def remove_user_from_group(group_id, user_name):
    request = proto.groups_pb2.RemoveUserRequest(groupID=group_id, userID=user_name)

    async with Channel(services.GROUP) as channel:
        stub = proto.groups_grpc.GroupServiceStub(channel)
        logging.info(f'Removing user {user_name} from group {group_id}')
        response = await stub.RemoveUser(request)
        logging.info(f'User {user_name} removed from group {group_id}')

    return response
