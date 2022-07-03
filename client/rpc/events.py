from typing import Optional, List

import grpclib.exceptions

from proto.history_pb2 import Action
from rpc import services
from rpc.client import Channel

from google.protobuf.timestamp_pb2 import Timestamp

import proto.events_grpc
import proto.events_pb2
from rpc.history import get_history
from rpc.requests_queue import add_request, Request, get_requests


async def create_event(start: Timestamp, end: Timestamp, name: str, description: str, group_id=0)\
        -> int:

    event = proto.events_pb2.Event(start=start, end=end, name=name, description=description, groupId=group_id)
    create_event_request = proto.events_pb2.CreateEventRequest(event=event)

    try:
        async with Channel(services.EVENT) as channel:
            stub = proto.events_grpc.EventsServiceStub(channel)
            response = await stub.CreateEvent(create_event_request)
    except grpclib.exceptions.GRPCError as err:
        if err.status == grpclib.const.Status.UNAVAILABLE:
            return 2
    except:
        add_request(Request(create_event_request, services.EVENT))
        return 1

    return 0


async def get_user_events():
    history = await get_history()

    events = dict()
    drafts = dict()

    # add event if event created, remove if event deleted
    for entry in history:
        if not entry.event:
            continue
        if entry.action == Action.CREATE:
            events[entry.event.id] = entry.event
            if entry.event.draft:
                drafts[entry.event.id] = entry.event
        if entry.action == Action.CONFIRM:
            del drafts[entry.event.id]
        if entry.action == Action.DELETE:
            del events[entry.event.id]
        if entry.action == Action.UPDATE:
            events[entry.event.id] = entry.event
            if entry.event.draft:
                del drafts[entry.event.id]

    local = get_requests()
    create: List[proto.events_pb2.CreateEventRequest] = [r.request for r in local if isinstance(r.request, proto.events_pb2.CreateEventRequest)]

    for event in create:
        events[event.event.id] = event.event

    reject: List[proto.events_pb2.RejectEventRequest] = [r.request for r in local if isinstance(r.request, proto.events_pb2.RejectEventRequest)]

    for event in reject:
        del events[event.eventId]
        del drafts[event.eventId]

    confirmed: List[proto.events_pb2.ConfirmEventRequest] = [r.request for r in local if isinstance(r.request, proto.events_pb2.ConfirmEventRequest)]

    for event in confirmed:
        del drafts[event.eventId]

    return events, drafts
