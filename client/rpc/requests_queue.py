import dataclasses
from typing import List

from rpc import services
from rpc.client import Channel
from store import Storage

from proto.events_pb2 import CreateEventRequest, ConfirmEventRequest, RejectEventRequest
from proto.events_grpc import EventsServiceStub


@dataclasses.dataclass
class Request:

    request: CreateEventRequest | ConfirmEventRequest | RejectEventRequest
    service: int


def add_request(request: Request):
    requests: List = Storage.disk_get('requests', [])
    requests.append(request)
    Storage.disk_store('requests', requests)


def get_requests() -> List[Request]:
    return Storage.disk_get('requests', [])


def remove_request(request: Request):
    requests: List = Storage.disk_get('requests', [])
    try:
        requests.remove(request)
    except ValueError:
        return
    Storage.disk_store('requests', requests)


def clear_requests():
    Storage.disk_store('requests', [])


async def process_requests():
    requests = get_requests()
    processed = []
    for request in requests:
        try:
            match request.service:
                case services.EVENT:
                    async with Channel(services.EVENT) as channel:
                        stub = EventsServiceStub(channel)
                        match request.request:
                            case CreateEventRequest():
                                response = await stub.CreateEvent(request.request)
                            case ConfirmEventRequest():
                                response = await stub.ConfirmEvent(request.request)
                            case RejectEventRequest():
                                response = await stub.RejectEvent(request.request)
        except:
            continue
        if response:
            processed.append(request)

    for req in processed:
        remove_request(req)
