import logging
from async_app_fw.controller.handler import observe_event, observe_event_from_self
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.event import event
from async_app_fw.controller.mcp_controller.mcp_state import \
    MC_DISCONNECT, MC_FEATURE, MC_HANDSHAK, MC_STABLE
from async_app_fw.controller.mcp_controller.master_controller import MachineControlMasterController
from async_app_fw.event.mcp_event import mcp_event

from .master_lib.constant import APP_NAME
from .master_lib.event import ReqGetAgentConnection
from .exception import AgentIsNotExist

LOG = logging.getLogger(
    'async_app_fw.controller.mcp_controller.agent_controller')

class MCPMasterHandler(BaseApp):
    _EVENTS = event.get_event_from_module(
        mcp_event)

    _EVENTS.append(ReqGetAgentConnection)

    def __init__(self, *_args, **_kwargs):
        super(MCPMasterHandler, self).__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.controller = None
        self.connection_dict = {}
        self.ip_to_connection = {}

    def start(self):
        task = super(MCPMasterHandler, self).start()
        self.controller = MachineControlMasterController()
        self.controller.start()
        return task

    async def close(self):
        await super().close()

        # cleanup controller and connection_dict
        connection_dict = self.connection_dict
        controller = self.controller
        self.controller = None
        self.connection_dict = {}

        # stop master controller first
        if  controller is not None \
            and isinstance(controller, MachineControlMasterController):
            await controller.stop()
            del controller

        # make sure connection serve stop
        if len(connection_dict) > 0:
            for _, connection in connection_dict.items():
                connection.set_state(MC_DISCONNECT)
                await connection.stop_serve()
                del connection

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def disconnecting_handler(self, ev: event.EventSocketConnecting):
        conn = ev.connection
        ip = conn.address[0]

        if conn.id in self.connection_dict:
            self.connection_dict.pop(conn.id)
            self.ip_to_connection[ip]

            del conn

    @observe_event_from_self(mcp_event.EventMCPHello, MC_HANDSHAK)
    def hello_handler(self, ev):
        self.logger.debug('hello ev %s', ev)
        conn = ev.msg.connection

        # check return connection_id
        # assert conn.id == ev.msg.connection_id
        LOG.info('get hello back')

        conn.set_state(MC_STABLE)
        self.connection_dict[conn.id] = conn
        self.ip_to_connection[conn.address[0]] = conn

    @observe_event(ReqGetAgentConnection)
    def get_agent_connection_handler(self, ev: ReqGetAgentConnection):
        result = self.ip_to_connection.get(ev.agent, None) or AgentIsNotExist(ev.agent)
        ev.push_reply(result)