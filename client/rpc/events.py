from rpc import services
from rpc.client import Channel

from google.protobuf.timestamp_pb2 import Timestamp

import proto.events_grpc
import proto.events_pb2


async def create_event(start: Timestamp, end: Timestamp, name: str, description: str, group_id=0)\
        -> proto.events_pb2.CreateEventResponse:

    event = proto.events_pb2.Event(start=start, end=end, name=name, description=description, groupId=group_id)
    create_event_request = proto.events_pb2.CreateEventRequest(event=event)

    async with Channel(services.EVENT) as channel:
        stub = proto.events_grpc.EventsServiceStub(channel)
        response = await stub.CreateEvent(create_event_request)

    return response
