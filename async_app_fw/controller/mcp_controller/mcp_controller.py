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

import logging
import random
import asyncio
from asyncio import CancelledError, StreamWriter, StreamReader, Task
from socket import IPPROTO_TCP, socket
from socket import TCP_NODELAY
from socket import SHUT_WR
from socket import timeout as SocketTimeout
import ssl

from async_app_fw.base.app_manager import BaseApp, lookup_service_brick
from async_app_fw.lib import hub
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.event import event

# from ryu import cfg
from async_app_fw.lib import ip
# from async_app_fw.base.app_manager import BaseApp, lookup_service_brick
from async_app_fw.protocol.mcp import mcp_common, mcp_parser, mcp_v_1_0 as mcproto, mcp_parser_v_1_0 as mcproto_parser

LOG = logging.getLogger(
    'eventlent_framework.controller.controller.mcp_controller')


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
    async def deactivate(self):
        try:
            await method(self)
        finally:
            try:
                self.writer.close()
                await self.writer.wait_closed()
                # self.socket.close()
            except IOError:
                pass

    return deactivate

class MachineConnectionIsServing(Exception):
    pass

class MachineConnection(object):
    def __init__(self, reader: StreamReader, writer: StreamWriter, mcp_brick_name):
        self.socket: socket = writer.get_extra_info('socket')
        self.address = writer.get_extra_info('peername')
        self.socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        self.reader = reader
        self.writer = writer
        self.is_active = True

        # The limit is arbitrary. We need to limit queue size to
        # prevent it from eating memory up.
        self.send_q = hub.Queue(1000)

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
        self._serve_task = None

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
        async def _async_recv_loop():
            buf = bytearray()
            min_read_len = remaining_read_len = mcp_common.MCP_HEADER_SIZE

            count = 0

            while self.state != MC_DISCONNECT:
                try:
                    read_len = min_read_len
                    if read_len > remaining_read_len:
                        read_len = remaining_read_len

                    ret = await self.reader.readexactly(read_len)
                except SocketTimeout:
                    LOG.warning('Socket timeout.')
                    continue
                except ssl.SSLError:
                    # eventlet throws SSLError (which is a subclass of IOError)
                    # on SSL socket read timeout; re-try the loop in this case.
                    continue
                except (EOFError, IOError):
                    break
                except CancelledError as e:
                    break

                if not ret:
                    break

                buf += ret
                buf_len = len(buf)

                while buf_len >= min_read_len:
                    (msg_type, msg_len, version_id, xid) = mcp_parser.header(buf)
                    if msg_len < min_read_len:
                        # Someone isn't playing nicely; log it, and try something sane.
                        LOG.debug("Message with invalid length %s received from Machine at address %s",
                                  msg_len, self.address)
                        msg_len = min_read_len
                    if buf_len < msg_len:
                        remaining_read_len = (msg_len - buf_len)
                        break

                    msg = mcp_parser.msg(
                        self, msg_type, msg_len, version_id, xid, buf[:msg_len])

                    if msg:
                        # decode event and create event
                        ev = mcp_event.mcp_msg_to_ev(msg)
                        if self.mcp_brick is not None:
                            # send event to observers and self event handlers
                            self.mcp_brick.send_event_to_observers(
                                ev, self.state)
                            self.mcp_brick.send_event_to_self(ev, self.state)

                    buf = buf[msg_len:]
                    buf_len = len(buf)
                    remaining_read_len = min_read_len

                    count += 1
                    if count > 2048:
                        count = 0
                        await asyncio.sleep(0)

        return hub.app_hub.spawn(_async_recv_loop)

    async def _send_loop(self):
        try:
            while self.state != MC_DISCONNECT:
                buf, close_socket = await self.send_q.get()
                self.writer.write(buf)
                await self.writer.drain()
                if close_socket:
                    break
        except CancelledError:
            LOG.debug("Stop _send_loop at address %s", self.address)
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
            self.send_q = None
            # First, clear self.send_q to prevent new references.
            # Now, drain the send_q, releasing the associated semaphore for each entry.
            # This should release all threads waiting to acquire the semaphore.
            try:
                # clean queue
                while q.get_nowait():
                    pass
            except asyncio.QueueEmpty:
                pass
            # Finally, disallow further sends.
            self._close_write()

    def send(self, buf, close_socket=False) -> Task:
        async def _send(buf, close_socket):
            msg_enqueued = False
            if self.send_q:
                await self.send_q.put((buf, close_socket))
                msg_enqueued = True

            if not msg_enqueued:
                LOG.debug('Machine Connection in process of terminating; send() to %s discarded.',
                          self.address)

            return msg_enqueued

        return hub.app_hub.spawn(_send, buf, close_socket)

    def set_xid(self, msg: mcproto_parser.MCPMsgBase):
        self.xid += 1
        self.xid &= self.mcproto.MAX_XID
        msg.set_xid(self.xid)
        return self.xid

    def send_msg(self, msg, close_socket=False) -> Task:
        async def _send_msg(msg, close_socket):
            assert isinstance(msg, self.mcproto_parser.MCPMsgBase)
            if msg.xid is None:
                self.set_xid(msg)
            msg.serialize()
            LOG.debug('send_msg %s', msg)
            return await self.send(msg.buf, close_socket=close_socket)
        return hub.app_hub.spawn(_send_msg, msg, close_socket)

    async def stop_serve(self):
        if isinstance(self._serve_task, asyncio.Task) and not self._serve_task.done():
            self._serve_task.cancel()
            await self._serve_task
        
        self._serve_task = None

    def serve(self):
        async def serve():
            send_loop_task = hub.app_hub.spawn(self._send_loop)
    
            # send connection event
            connect_ev = event.EventSocketConnecting(self)
    
            if self.mcp_brick is not None:
                self.mcp_brick.send_event_to_observers(connect_ev)
                handlers = self.mcp_brick.get_handlers(
                    connect_ev, MC_HANDSHAK)
    
                for handler in handlers:
                    handler(connect_ev)
    
            # send socket connecting event
            exception = None
            try:
                recv_loop_task = hub.app_hub.spawn(self._recv_loop)
                await recv_loop_task
            except Exception as e:
                exception = e
            finally:
                hub.app_hub.kill(send_loop_task)
                hub.app_hub.kill(recv_loop_task)
                self.is_active = False
                if exception is not None:
                    raise exception
        
        if self._serve_task is not None:
            raise MachineConnectionIsServing()
        
        self._serve_task = hub.app_hub.spawn(serve)

        return self._serve_task