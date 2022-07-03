import ipaddress
import logging
import random

import jwt
import psutil
import socket
from grpclib.client import Channel as BaseChannel
from grpclib.events import SendRequest, listen, RecvTrailingMetadata
from grpclib.const import Status
from grpclib.protocol import H2Protocol

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

    async def __connect__(self) -> H2Protocol:
        while True:
            try:
                return await super().__connect__()
            except OSError as err:
                logging.error("Error connecting to server %s:%s", self._host, self._port)
                try:
                    Storage.get('server', ['localhost']).remove(self._host)
                except KeyError:
                    pass
                except ValueError:
                    pass
                if len(Storage.get('server', [])) != 0:
                    self._host = get_host()
                    logging.info("Trying to connect to %s:%s", self._host, self._port)
                else:
                    logging.error("No servers available")
                    raise err


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
        sock.settimeout(1)
        sock.sendto(b'Chord?', (broadcast, 8830))

        response = None
        address = None
        while True:
            try:
                logging.info("Waiting for response")
                response, address = sock.recvfrom(1024)
                logging.info(f"Received {response} from {address}")
                if response == b"I am chord":
                    yield address[0]
            except socket.timeout:
                logging.info("Timeout")
                break
            except BaseException as e:
                logging.info(f"Error: {e}")
                break
        try:
            sock.close()
        except BaseException as e:
            logging.info(f"Error closing socket: {e}")

        if response and address:
            break
    logging.info("No more servers found")


def update_servers():
    servers = list(discover())
    logging.info(f"Found servers: {servers}")
    if servers:
        logging.info("Updating servers")
        Storage.store('server', servers)
    else:
        logging.info("No servers found")
        Storage.delete('server')
