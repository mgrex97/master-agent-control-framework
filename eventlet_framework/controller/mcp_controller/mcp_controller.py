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
from eventlet_framework.event import event

# from ryu import cfg
from eventlet_framework.lib import hub
from eventlet_framework.lib.hub import StreamServer
from eventlet_framework.lib import ip
from eventlet_framework.base.app_manager import BaseApp, lookup_service_brick
from eventlet_framework.protocol.mcp import mcp_common, mcp_parser, mcp_v_1_0 as mcproto, mcp_parser_v_1_0 as mcproto_parser

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


class MachineConnection(object):
    def __init__(self, socket: socket, address, mcp_brick_name):
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

        self.mcproto_parser = mcproto_parser
        self.mcproto = mcproto
        # not finished yet
        self.xid = random.randint(0, self.mcproto.MAX_XID)
        # self.mcp_proto.MAX_XID)
        self.id = 0  # machine_id is unknown yet
        self.state = None
        self.mcp_brick: BaseApp = lookup_service_brick(
            mcp_brick_name)
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
            handlers = self.mcp_brick.get_handlers(ev, state)

            for handler in handlers:
                handler(ev)

    @_deactivate
    def _recv_loop(self):
        buf = bytearray()
        min_read_len = remaining_read_len = mcp_common.MCP_HEADER_SIZE

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
                        # send event to observers and self event handlers
                        self.mcp_brick.send_event_to_observers(ev, self.state)
                        handlers = self.mcp_brick.get_handlers(ev, self.state)

                        for handler in handlers:
                            handler(ev)

                buf = buf[msg_len:]
                buf_len = len(buf)
                remaining_read_len = min_read_len

                count += 1
                if count > 2048:
                    count = 0
                    hub.sleep(0)

    def _send_loop(self):
        try:
            while self.state != MC_DISCONNECT:
                buf, close_socket = self.send_q.get()
                self._send_q_sem.release()
                self.socket.sendall(buf)
                if close_socket:
                    break
        except SocketTimeout:
            LOG.debug("Socket timed out while sending data to switch at address %s",
                      self.address)
        except IOError as ioe:
            # Convert ioe.errno to a string, just in case it was somehow set to None.
            errno = "%s" % ioe.errno
            LOG.debug("Socket error while sending data to switch at address %s: [%s] %s",
                      self.address, errno, ioe.strerror)
        finally:
            q = self.send_q
            # First, clear self.send_q to prevent new references.
            self.send_q = None
            # Now, drain the send_q, releasing the associated semaphore for each entry.
            # This should release all threads waiting to acquire the semaphore.
            try:
                while q.get(block=False):
                    self._send_q_sem.release()
            except hub.QueueEmpty:
                pass
            # Finally, disallow further sends.
            self._close_write()

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

    def set_xid(self, msg: mcproto_parser.MCPMsgBase):
        self.xid += 1
        self.xid &= self.mcproto.MAX_XID
        msg.set_xid(self.xid)
        return self.xid

    def send_msg(self, msg, close_socket=False):
        assert isinstance(msg, self.mcproto_parser.MCPMsgBase)
        if msg.xid is None:
            self.set_xid(msg)
        msg.serialize()
        # LOG.debug('send_msg %s', msg)
        return self.send(msg.buf, close_socket=close_socket)

    def serve(self):
        send_thr = hub.spawn(self._send_loop)

        # send connection event
        connect_ev = event.EventSocketConnecting(self)

        # send socket connecting event
        self.mcp_brick.send_event_to_observers(connect_ev)
        handlers = self.mcp_brick.get_handlers(
            connect_ev, MC_HANDSHAK)

        for handler in handlers:
            handler(connect_ev)

        try:
            self._recv_loop()
        finally:
            hub.kill(send_thr)
            hub.joinall([send_thr])
            self.is_active = False
