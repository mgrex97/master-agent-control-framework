from eventlet_framework.controller.handler import observe_event
from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.controller.mcp_controller.mcp_controller import MachineControlMasterController
from eventlet_framework.lib import hub
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.mcp_controller.mcp_state import MC_HANDSHAK


class MCPHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super(MCPHandler, self).__init__(*_args, **_kwargs)
        self.name = 'mcp_handler'
        self.controller = None

    def start(self):
        super(MCPHandler, self).start()
        self.controller = MachineControlMasterController()
        return hub.spawn(self.controller)

    @observe_event(mcp_event.EventMCPHello, MC_HANDSHAK)
    def hello_handler(self, ev):
        self.logger.debug('hello ev %s', ev)
