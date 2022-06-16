import logging
from eventlet_framework.controller.handler import observe_event, observe_event_from_self
from eventlet_framework.base.async_app_manager import BaseApp
from eventlet_framework.controller.mcp_controller.async_ver.master_controller import MachineControlMasterController
from eventlet_framework.lib.async_hub import app_hub
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.event import event
from eventlet_framework.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_FEATURE, MC_HANDSHAK, MC_STABLE

LOG = logging.getLogger(
    'eventlent_framework.controller.mcp_controller.agent_controller')


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

        app_hub.spawn(self.controller.server_loop)
        return task

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
