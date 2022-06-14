from platform import machine
from socket import TCP_NODELAY
from socket import IPPROTO_TCP
import traceback
import logging
import contextlib
from asyncio import CancelledError, StreamWriter, StreamReader

from yaml import add_representer

from eventlet_framework.controller.mcp_controller.async_ver.mcp_controller import MachineConnection
from eventlet_framework.controller.mcp_controller.async_ver.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from eventlet_framework.lib import async_hub
from eventlet_framework.lib.async_hub import app_hub

LOG = logging.getLogger(
    'eventlent_framework.controller.mcp_controller.master_controller')


class MachineControlMasterController(object):
    def __init__(self, listen_host='127.0.0.1', listen_port=7930):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self._clients = {}

    async def server_loop(self):
        server = async_hub.StreamServer(
            (self.listen_host, self.listen_port), machine_connection_factory)
        await server.serve_forever()


class MasterConnection(MachineConnection):
    # serial number
    machine_id = 1

    def __init__(self, socket, address, mcp_brick_name='mcp_master_handler'):
        super(MasterConnection, self).__init__(
            socket, address, mcp_brick_name=mcp_brick_name)

    def _get_new_machine_id(self):
        m_id = self.machine_id
        self.machine_id = self.machine_id + 1
        return m_id

    async def serve(self):
        if self.id == 0:
            self.id = self._get_new_machine_id()

        msg_hello = self.mcproto_parser.MCPHello(self, self.id)

        await self.send_msg(msg_hello)
        await super().serve()


async def machine_connection_factory(reader: StreamReader, writer: StreamWriter):
    socket = writer.get_extra_info('socket')
    address = socket.getpeername()
    LOG.info('connected socket: address:%s port:%s',
             *address)
    with contextlib.closing(MasterConnection(reader, writer)) as machine_connection:
        try:
            await app_hub.spawn(machine_connection.serve)
        except Exception as e:
            # Something went wrong.
            # Especially malicious switch can send malformed packet,
            # the parser raise exception.
            # Can we do anything more graceful?
            # print(e)
            if machine_connection.id is None:
                dpid_str = "(id not been set yet)"
            else:
                dpid_str = str(machine_connection.id)
            LOG.error("Error in the datapath %s from %s:%s\nError Detail: %s",
                      dpid_str, *address, traceback.format_exc())
        except CancelledError:
            LOG.info("Connection cancel.")
        finally:
            LOG.info(f"Connection from {address[0]}:{address[1]} disconnect.")
            machine_connection.set_state(MC_DISCONNECT)
