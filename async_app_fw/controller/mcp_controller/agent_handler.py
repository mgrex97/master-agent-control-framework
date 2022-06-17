import logging
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from async_app_fw.event import event
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.controller.handler import observe_event, observe_event_from_self
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK, MC_STABLE
from async_app_fw.controller.mcp_controller.agent_controller import MachineControlAgentController

LOG = logging.getLogger(
    'eventlent_framework.controller.mcp_controller.agent_controller')


class AgentMCPHandler(BaseApp):
    _EVENTS = event.get_event_from_module(
        mcp_event)

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'mcp_agent_handler'
        self.controller = None

    def start(self):
        task = super().start()
        self.controller = MachineControlAgentController()
        return [app_hub.spawn(self.controller.attempt_connecting_loop, interval=2), task]

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def disconnecting_handler(self, ev: event.EventSocketConnecting):
        LOG.info('disconnect')

    @observe_event_from_self(mcp_event.EventMCPHello, MC_HANDSHAK)
    def hello_handler(self, ev):
        LOG.info('agent get hello')
        # machine id sync
        conn = ev.msg.connection
        conn.id = ev.msg.connection_id
        msg = conn.mcproto_parser.MCPHello(conn, conn.id)
        conn.send_msg(msg)
        conn.set_state(MC_STABLE)
