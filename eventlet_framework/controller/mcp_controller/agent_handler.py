from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.lib import hub
from eventlet_framework.event import event
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event
from eventlet_framework.controller.mcp_controller.mcp_state import MC_HANDSHAK
from eventlet_framework.controller.mcp_controller.agent_controller import AgentConnection, MachineControlAgentController


class AgentMCPHandler(BaseApp):
    _EVENTS = event.get_event_from_module(
        mcp_event)

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'mcp_agent_handler'
        self.controller = None

    def start(self):
        super().start()
        self.controller = MachineControlAgentController()
        return hub.spawn(self.controller)

    @observe_event(event.EventSocketConnecting, MC_HANDSHAK)
    def connecting_handler(self, ev: event.EventSocketConnecting):
        pass

    @observe_event(mcp_event.EventMCPHello, MC_HANDSHAK)
    def hello_handler(self, ev):
        print('get hello msg')
        pass
