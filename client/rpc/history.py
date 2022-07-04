import json
import logging
from typing import List

from google.protobuf.json_format import MessageToJson, Parse
from grpclib import GRPCError

import proto.history_grpc
import proto.history_pb2
from store import Storage
from . import services
from .client import Channel, TOKEN, get_user

HISTORY = 'history'


class HistoryEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, proto.history_pb2.HistoryEntry):
            return json.loads(MessageToJson(obj, including_default_value_fields=True))
        return json.JSONEncoder.default(self, obj)


class HistoryDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if 'action' in obj:
            return Parse(json.dumps(obj), proto.history_pb2.HistoryEntry())
        return obj


async def update_history():
    if not await Storage.async_disk_get(TOKEN):
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
                history = [r.entry for r in response]
                await Storage.async_disk_store(HISTORY, json.dumps(history, cls=HistoryEncoder, indent=2))
            except GRPCError as err:
                logging.error(f'A GRPCError error occurred retrieving the full history: {err.status}: {err.message}')
                # raise err
            except OSError as err:
                logging.error(f'An OSError error occurred retrieving the full history: {err}')
                # raise err

        else:
            request = proto.history_pb2.GetHistoryFromOffsetRequest(username=user.get('name'), offset=offset)
            logging.info(f'Retrieving history from offset {offset}')
            try:
                response = await stub.GetHistoryFromOffset(request)
                logging.info(f'History retrieved from offset {offset}: {response}')
                history.extend([r.entry for r in response])
                await Storage.async_disk_store(HISTORY, json.dumps(history, cls=HistoryEncoder, indent=2))
            except GRPCError as err:
                logging.error(f'An error occurred retrieving the history from offset {offset}: {err.status}: {err.message}')
                # raise err
            except OSError as err:
                logging.error(f'An error occurred retrieving the history from offset {offset}: {err}')
                # raise err


async def get_history() -> List[proto.history_pb2.HistoryEntry]:
    try:
        history = json.loads(await Storage.async_disk_get(HISTORY), cls=HistoryDecoder)
    except TypeError:
        history = []
    except ValueError:
        history = []
    # logging.info(f'History loaded from disk: {history[len(history) - 2:]}')
    return history



