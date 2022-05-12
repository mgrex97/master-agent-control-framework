# Copyright (C) 2011, 2012 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011, 2012 Isaku Yamahata <yamahata at valinux co jp>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The main component of Wiresahrk controller.

- Handle connections from client

"""

import contextlib
import logging
import random
from socket import socket
from socket import IPPROTO_TCP
from socket import TCP_NODELAY
from socket import SHUT_WR
from socket import timeout as SocketTimeout
import ssl
from eventlet_framework.controller.tshark_controller import tshark_event

# from ryu import cfg
from eventlet_framework.lib import hub
from eventlet_framework.lib.hub import StreamServer
from eventlet_framework.lib import ip
from eventlet_framework.base.app_manager import lookup_service_brick

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(
    'eventlent_framework.controller.tshark.tshark_controller')

DEFAULT_BATCH_SIZE = 2 ** 16


def _split_addr(addr):
    """
    Splits a str of IP address and port pair into (host, port).

    Example::

        >>> _split_addr('127.0.0.1:6653')
        ('127.0.0.1', 6653)
        >>> _split_addr('[::1]:6653')
        ('::1', 6653)

    Raises ValueError if invalid format.

    :param addr: A pair of IP address and port.
    :return: IP address and port
    """
    e = ValueError('Invalid IP address and port pair: "%s"' % addr)
    pair = addr.rsplit(':', 1)
    if len(pair) != 2:
        raise e

    addr, port = pair
    if addr.startswith('[') and addr.endswith(']'):
        addr = addr.lstrip('[').rstrip(']')
        if not ip.valid_ipv6(addr):
            raise e
    elif not ip.valid_ipv4(addr):
        raise e

    return addr, int(port, 0)


def _deactivate(method):
    def deactivate(self):
        try:
            method(self)
        finally:
            try:
                self.socket.close()
            except IOError:
                pass

    return deactivate


class TsharkController(object):
    def __init__(self):
        self.tshark_tcp_listen_port = 2235
        self.tshark_listen_host = '169.254.0.111'
        hub.spawn(self.server_loop, self.tshark_tcp_listen_port)
        self._clients = {}

    def __call__(self):
        self.server_loop(self.tshark_tcp_listen_port)

    def server_loop(self, tshark_tcp_listen_port):
        server = StreamServer(
            (self.tshark_listen_host, tshark_tcp_listen_port), tshark_connection_factory)
        server.serve_forever()


class TsharkStreamConnection(object):
    def __init__(self, socket: socket, address):
        self.socket = socket
        self.socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        self.socket.settimeout(10.0)
        self.address = address
        self.is_active = True

        # The limit is arbitrary. We need to limit queue size to
        # prevent it from eating memory up.
        self.send_q = hub.Queue(16)
        self._send_q_sem = hub.BoundedSemaphore(self.send_q.maxsize)

        self.unreplied_echo_requests = []

        self.id = None  # datapath_id is unknown yet
        self._ports = None
        self.state = True
        self.tshark_brick = lookup_service_brick(
            'tshark_event')

    def _close_write(self):
        # Note: Close only further sends in order to wait for the switch to
        # disconnect this connection.
        try:
            self.socket.shutdown(SHUT_WR)
        except (EOFError, IOError):
            pass

    def close(self):
        self._close_write()

    # Low level socket handling layer
    @_deactivate
    def _recv_loop(self):
        min_read_len = remaining_read_len = DEFAULT_BATCH_SIZE

        packet_count = 0
        buf = bytearray()

        while self.state is True:
            try:
                # read_len = min_read_len
                # if remaining_read_len > min_read_len:
                # read_len = remaining_read_len
                ret = self.socket.recv(min_read_len)
            except SocketTimeout:
                continue
            except ssl.SSLError:
                # eventlet throws SSLError (which is a subclass of IOError)
                # on SSL socket read timeout; re-try the loop in this case.
                continue
            except (EOFError, IOError):
                break

            if not ret:
                break

            buf += ret

            packet, buf = tshark_event.extract_packet_json_from_data(
                buf, got_first_packet=packet_count > 0)

            if packet is None:
                continue

            # tshark_packet_in
            ev_type = 'tshark.packet_in'
            ev = tshark_event.tshark_packet_to_ev_cls(packet, self.address)

            if self.tshark_brick is not None:
                self.tshark_brick.send_event_to_observers(ev, ev_type)

                def ev_types(x):
                    return x.callers[ev.__class__].ev_types

                handlers = [handler for handler in
                            self.tshark_brick.get_handlers(ev) if
                            ev_type in ev_types(handler)]

                for handler in handlers:
                    handler(ev)

    def serve(self):
        try:
            self._recv_loop()
        finally:
            # hub.joinall([green_thr])
            self.is_active = False


def tshark_connection_factory(socket: socket, address):
    LOG.debug('connected socket:%s address:%s port:%s',
              socket, *socket.getpeername())
    with contextlib.closing(TsharkStreamConnection(socket, address)) as tshark_client_socket:
        try:
            tshark_client_socket.serve()
        except Exception as e:
            # Something went wrong.
            # Especially malicious switch can send malformed packet,
            # the parser raise exception.
            # Can we do anything more graceful?
            print(e)
            """
            if datapath.id is None:
                dpid_str = "%s" % datapath.id
            else:
                dpid_str = dpid_to_str(datapath.id)
            LOG.error("Error in the datapath %s from %s", dpid_str, address)
            raise
            """
