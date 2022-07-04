import dataclasses
import logging
from typing import List

import grpclib.exceptions

from rpc import services
from rpc.client import Channel
from rpc.history import update_history
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
        pass
    try:
        Storage.get('conflicts', []).remove(request)
    except ValueError:
        pass
    Storage.disk_store('requests', requests)


def clear_requests():
    Storage.disk_store('requests', [])
    Storage.store('conflicts', [])


async def process_requests():
    requests = get_requests()
    logging.info(f"Processing requests: {requests}")
    processed = []
    conflicts = Storage.get('conflicts', [])
    for request in requests:
        logging.info(f"Pushing request: {request}")
        if request in conflicts:
            logging.info("Conflict detected, skipping request")
            continue
        response = None
        try:
            match request.service:
                case services.EVENT:
                    logging.info("Request is for event service")
                    async with Channel(services.EVENT) as channel:
                        stub = EventsServiceStub(channel)
                        match request.request:
                            case CreateEventRequest():
                                try:
                                    response = await stub.CreateEvent(request.request)
                                except grpclib.exceptions.GRPCError as err:
                                    if err.status == grpclib.const.Status.UNAVAILABLE:
                                        logging.info("Event is in conflict, adding to conflicts")
                                        conflicts.append(request)
                                    raise err
                            case ConfirmEventRequest():
                                logging.info(f"Confirming event {request.request}")
                                response = await stub.ConfirmEvent(request.request)
                            case RejectEventRequest():
                                logging.info(f"Rejecting event {request.request}")
                                response = await stub.RejectEvent(request.request)
                case _:
                    logging.info("Request is for unknown service")
        except BaseException as err:
            logging.exception(f"Request processing failed with exception: {err}")
            continue
        if response:
            processed.append(request)

    for req in processed:
        remove_request(req)

    Storage.store('conflicts', conflicts)

    if processed:
        await update_history()
