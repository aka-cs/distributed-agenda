import ipaddress
import logging
import random

import jwt
import psutil
import socket
from grpclib.client import Channel as BaseChannel
from grpclib.events import SendRequest, listen, RecvTrailingMetadata
from grpclib.const import Status
from store import Storage


def get_host():
    return random.choice(Storage.get('server', ['localhost']))


TOKEN = 'token'


def get_user():
    token = Storage.disk_get(TOKEN)
    if not token:
        return None

    with open('pub.pem', 'rb') as pub:
        public_key = pub.read()

    info = jwt.decode(token, public_key, algorithms=['RS256'])

    return info


class Channel(BaseChannel):

    def __init__(self, *args, **kwargs):
        host = get_host()
        logging.info('Connecting to %s', host)
        super(Channel, self).__init__(host, *args, **kwargs)

        listen(self, SendRequest, self.on_send_request)
        listen(self, RecvTrailingMetadata, self.on_recv_trailing_metadata)

    @staticmethod
    async def on_send_request(event: SendRequest):
        token = await Storage.async_disk_get('token')
        if token:
            logging.info("Adding token to request")
            event.metadata['authorization'] = token

    @staticmethod
    async def on_recv_trailing_metadata(event: RecvTrailingMetadata):
        if event.status == Status.UNAUTHENTICATED:
            logging.info("Token expired")
            await Storage.async_disk_delete('token')


async def logout():
    Storage.clear()


def get_ipv4_addresses():
    family = socket.AF_INET
    for interface, snics in psutil.net_if_addrs().items():
        for snic in snics:
            if snic.family == family:
                if snic.address.startswith('169.254'):
                    continue
                ipv4 = ipaddress.ip_address(snic.address)
                if ipv4.is_loopback:
                    continue
                yield snic.address


def discover():
    for ip in get_ipv4_addresses():
        ip_numbers = ip.split('.')
        ip_numbers[3] = '255'
        broadcast = '.'.join(ip_numbers)
        logging.info(f"Discovering on {broadcast}")

        # udp broadcast on broadcast address
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(b'Hello', (broadcast, 8830))

        response = None
        address = None
        while True:
            try:
                logging.info("Waiting for response")
                response, address = sock.recvfrom(1024)
                break
            except BaseException as e:
                logging.info(f"Error: {e}")
                break
        try:
            sock.close()
        except BaseException as e:
            logging.info(f"Error closing socket: {e}")

        if response and address:
            logging.info(f"Received {response} from {address}")
            return [address[0]]
    logging.info("No more servers found")


def update_servers():
    servers = list(discover())
    logging.info(f"Found servers: {servers}")
    if servers:
        logging.info("Updating servers")
        Storage.store('server', servers)