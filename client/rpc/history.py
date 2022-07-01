import json
import logging

from grpclib import GRPCError

import proto.history_grpc
import proto.history_pb2
from store import Store
from . import services
from .client import Channel, TOKEN, get_user

HISTORY = 'history'


async def update_history():
    if not await Store.async_disk_get(TOKEN):
        return

    history = await get_history()
    offset = len(history)
    user = get_user()

    async with Channel(services.HISTORY) as channel:
        stub = proto.history_grpc.HistoryServiceStub(channel)

        if offset == 0:
            request = proto.history_pb2.GetFullHistoryRequest(username=user.get('sub'))
            logging.info('Retrieving full history')
            try:
                response = await stub.GetFullHistory(request)
                logging.info(f'Full history retrieved: {response}')
                await Store.async_disk_store(HISTORY, json.dumps(response))
            except GRPCError as err:
                logging.error(f'An error occurred retrieving the full history: {err.status}: {err.message}')
                raise err

        else:
            request = proto.history_pb2.GetHistoryFromOffsetRequest(username=user.get('name'), offset=offset)
            logging.info(f'Retrieving history from offset {offset}')
            try:
                response = await stub.GetHistoryFromOffset(request)
                logging.info(f'History retrieved from offset {offset}: {response}')
                history.extend(response)
                await Store.async_disk_store(HISTORY, json.dumps(history))
            except GRPCError as err:
                logging.error(f'An error occurred retrieving the history from offset {offset}: {err.status}: {err.message}')
                raise err


async def get_history():
    try:
        history = json.loads(await Store.async_disk_get(HISTORY))
    except TypeError:
        history = []
    except ValueError:
        history = []
    return history



