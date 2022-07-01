import logging

import jwt
from grpclib.client import Channel as BaseChannel
from grpclib.events import SendRequest, listen, RecvTrailingMetadata
from grpclib.const import Status
from store import Store


def get_host():
    return 'localhost'


TOKEN = 'token'


def get_user():
    token = Store.disk_get(TOKEN)
    if not token:
        return None

    with open('pub.pem', 'rb') as pub:
        public_key = pub.read()

    info = jwt.decode(token, public_key, algorithms=['RS256'])

    return info


class Channel(BaseChannel):

    def __init__(self, *args, **kwargs):
        host = get_host()
        super(Channel, self).__init__(host, *args, **kwargs)

        listen(self, SendRequest, self.on_send_request)
        listen(self, RecvTrailingMetadata, self.on_recv_trailing_metadata)

    @staticmethod
    async def on_send_request(event: SendRequest):
        token = await Store.async_disk_get('token')
        if token:
            logging.info("Adding token to request")
            event.metadata['authorization'] = token

    @staticmethod
    async def on_recv_trailing_metadata(event: RecvTrailingMetadata):
        if event.status == Status.UNAUTHENTICATED:
            logging.info("Token expired")
            await Store.async_disk_delete('token')


async def logout():
    Store.clear()
