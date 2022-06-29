from grpclib.client import Channel as BaseChannel


def get_host():
    return 'localhost'


class Channel(BaseChannel):

    def __init__(self, *args, **kwargs):
        host = get_host()
        super(Channel, self).__init__(host, *args, **kwargs)
