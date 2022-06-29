import logging

from grpclib.client import Channel as BaseChannel
from grpclib.events import SendRequest, listen
from store import Store


def get_host():
    return 'localhost'


TOKEN = 'token'


class Channel(BaseChannel):

    def __init__(self, *args, **kwargs):
        host = get_host()
        super(Channel, self).__init__(host, *args, **kwargs)

        listen(self, SendRequest, self.on_send_request)

    @staticmethod
    async def on_send_request(event: SendRequest):
        token = await Store.async_disk_get('token')
        if token:
            logging.info("Adding token to request")
            event.metadata['Authorization'] = 'Bearer ' + token
