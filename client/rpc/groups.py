from rpc import services
from rpc.client import Channel
from rpc.history import get_history

import proto.groups_grpc
import proto.groups_pb2

async def get_all_groups():
    history = await get_history()

    groups = set()

    # add group if group created, remove if group deleted
    for event in history:
        pass  # TODO: implement

    return groups


async def create_group(users):

    new_group = proto.groups_pb2.Group()
    request = proto.groups_pb2.CreateGroupRequest(group=new_group, users=users)

    async with Channel(services.GROUP) as channel:
        stub = proto.groups_grpc.GroupServiceStub(channel)
        await stub.CreateGroup(request)

    return
