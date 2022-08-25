import logging
from async_app_fw.controller.handler import observe_event, observe_event_from_self
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.mcp_controller.master_controller import MachineControlMasterController
from async_app_fw.lib.hub import app_hub
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.event import event
from async_app_fw.controller.mcp_controller.mcp_state import \
    MC_DISCONNECT, MC_FEATURE, MC_HANDSHAK, MC_STABLE

LOG = logging.getLogger(
    'async_app_fw.controller.mcp_controller.agent_controller')


class MCPMasterHandler(BaseApp):
    _EVENTS = event.get_event_from_module(
        mcp_event)

    def __init__(self, *_args, **_kwargs):
        super(MCPMasterHandler, self).__init__(*_args, **_kwargs)
        self.name = 'mcp_master_handler'
        self.controller = None
        self.connection_dict = {}

    def start(self):
        task = super(MCPMasterHandler, self).start()
        self.controller = MachineControlMasterController()
        self.controller.start()
        return task

    async def close(self):
        await super().close()

        for _, connection in self.connection_dict.items():
            # connection.set_state(MC_DISCONNECT)
            await connection.stop_serve()

        del self.connection_dict
        self.connection_dict = {}

        if  self.controller is not None \
            and isinstance(self.controller, MachineControlMasterController):

            await self.controller.stop()
            del self.controller
            self.controller = None

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def disconnecting_handler(self, ev: event.EventSocketConnecting):
        LOG.info('disconnect')
        """
        conn = ev.connection
        if conn.id in self.connection_dict:
            del ev.connection
        """

    @observe_event_from_self(mcp_event.EventMCPHello, MC_HANDSHAK)
    def hello_handler(self, ev):
        self.logger.debug('hello ev %s', ev)
        conn = ev.msg.connection

        # check return connection_id
        # assert conn.id == ev.msg.connection_id
        LOG.info('get hello back')

        conn.set_state(MC_STABLE)
        self.connection_dict[conn.id] = conn
