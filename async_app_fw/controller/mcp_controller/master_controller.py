import traceback
import logging
import contextlib
from asyncio import CancelledError, StreamWriter, StreamReader

from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from async_app_fw.lib import hub
from async_app_fw.lib.hub import app_hub

LOG = logging.getLogger(
    'eventlent_framework.controller.mcp_controller.master_controller')


class MachineControlMasterController(object):
    def __init__(self, listen_host='127.0.0.1', listen_port=7930):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self._clients = {}
        self._server_loop_task = None

    def start(self):
        self._server_loop_task = app_hub.spawn(self.server_loop)

    async def stop(self):
        if self._server_loop_task is not None:
            # wait server_loop stop.
            self._server_loop_task.cancel()
            await self._server_loop_task

    async def server_loop(self):
        self._server = hub.StreamServer(
            (self.listen_host, self.listen_port), machine_connection_factory)

        await self._server.serve_forever()


class MasterConnection(MachineConnection):
    # serial number
    MACHINE_ID = 0

    def __init__(self, socket, address, mcp_brick_name='mcp_master_handler'):
        super(MasterConnection, self).__init__(
            socket, address, mcp_brick_name=mcp_brick_name)

    @classmethod
    def _get_new_machine_id(cls):
        m_id = cls.MACHINE_ID
        cls.MACHINE_ID = cls.MACHINE_ID + 1
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
            connection_serve = app_hub.spawn(machine_connection.serve)
            await connection_serve
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
            connection_serve.cancel()
            machine_connection.set_state(MC_DISCONNECT)
