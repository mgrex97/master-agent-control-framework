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
from eventlet_framework.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from eventlet_framework.event.mcp_event import mcp_event

# from ryu import cfg
from eventlet_framework.lib import hub
from eventlet_framework.lib.hub import StreamServer
from eventlet_framework.lib import ip
from eventlet_framework.base.app_manager import BaseApp, lookup_service_brick
from eventlet_framework.protocol.mcp import mcp_common, mcp_parser, mcp_v_1_0 as mcproto

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(
    'eventlent_framework.controller.tshark.tshark_controller')


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


class MachineControlMasterController(object):
    def __init__(self):
        self.tcp_listen_port = 7930
        self.listen_host = '169.254.0.111'
        hub.spawn(self.server_loop, self.tcp_listen_port)
        self._clients = {}

    def __call__(self):
        self.server_loop(self.tcp_listen_port)

    def server_loop(self, tshark_tcp_listen_port):
        server = StreamServer(
            (self.listen_host, tshark_tcp_listen_port), machine_connection_factory)
        server.serve_forever()


class MachineConnection(object):
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

        self.mcproto = mcproto
        # not finished yet
        self.xid = random.randint(0, self.mc_proto.MAX_XID)
        # self.mcp_proto.MAX_XID)
        self.id = None  # machine_id is unknown yet
        self.state = None
        self.mcp_brick: BaseApp = lookup_service_brick(
            'mcp_handler')
        self.set_state(MC_HANDSHAK)

    def _close_write(self):
        # Note: Close only further sends in order to wait for the switch to
        # disconnect this connection.
        try:
            self.socket.shutdown(SHUT_WR)
        except (EOFError, IOError):
            pass

    def close(self):
        self._close_write()

    def set_state(self, state):
        if self.state == state:
            return

        ev = mcp_event.EventMCPStateChange(self, state, self.state)
        # change state before send.
        self.state = state

        if self.mcp_brick != None:
            self.mcp_brick.send_event_to_observers(ev, state)

    @_deactivate
    def _recv_loop(self):
        buf = bytearray()
        min_read_len = remaining_read_len = mcp_common.MCP_HEADER_PACK_STR

        count = 0

        while self.state != MC_DISCONNECT:
            try:
                read_len = min_read_len
                if remaining_read_len > min_read_len:
                    read_len = remaining_read_len
                ret = self.socket.recv(read_len)
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
            buf_len = len(buf)

            while buf_len >= min_read_len:
                (machine_id, msg_type, msg_len, xid) = mcp_parser.header(buf)
                if msg_len < min_read_len:
                    # Someone isn't playing nicely; log it, and try something sane.
                    LOG.debug("Message with invalid length %s received from Machine at address %s",
                              msg_len, self.address)
                    msg_len = min_read_len
                if buf_len < msg_len:
                    remaining_read_len = (msg_len - buf_len)
                    break

                msg = mcp_parser.msg(
                    self, msg_type, msg_len, xid, buf[:msg_len])

                if msg:
                    # decode event and create event
                    ev = mcp_event.mcp_msg_to_ev(msg)
                    if self.mcp_brick is not None:
                        self.mcp_brick.send_event_to_observers(ev, self.state)

                        def ev_types(x):
                            return x.callers[ev.__class__].ev_types

                        handlers = [handler for handler in
                                    self.mcp_brick.get_handlers(ev) if
                                    self.state in ev_types(handler)]

                        for handler in handlers:
                            handler(ev)

                buf = buf[msg_len:]
                buf_len = len(buf)
                remaining_read_len = min_read_len

                count += 1
                if count > 2048:
                    count = 0
                    hub.sleep(0)

    def send(self, buf, close_socket=False):
        msg_enqueued = False
        self._send_q_sem.acquire()
        if self.send_q:
            self.send_q.put((buf, close_socket))
            msg_enqueued = True
        else:
            self._send_q_sem.release()
        if not msg_enqueued:
            LOG.debug('Machine Connection in process of terminating; send() to %s discarded.',
                      self.address)
        return msg_enqueued

    def set_xid(self, msg: mcp_parser.MCPMsgBase):
        self.xid += 1
        self.xid &= self.mcproto.MAX_XID
        msg.set_xid(self.xid)
        return self.xid

    def send_msg(self, msg, close_socket=False):
        assert isinstance(msg, mcp_parser.MCPMsgBase)
        if msg.xid is None:
            self.set_xid(msg)
        msg.serialize()
        # LOG.debug('send_msg %s', msg)
        return self.send(msg.buf, close_socket=close_socket)

    def serve(self):
        try:
            self._recv_loop()
        finally:
            # hub.joinall([green_thr])
            self.is_active = False


def machine_connection_factory(socket: socket, address):
    LOG.debug('connected socket:%s address:%s port:%s',
              socket, *socket.getpeername())
    with contextlib.closing(MachineConnection(socket, address)) as machine_connection:
        try:
            machine_connection.serve()
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
