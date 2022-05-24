from eventlet_framework.controller.handler import observe_event
from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.controller.mcp_controller.master_controller import MachineControlMasterController
from eventlet_framework.lib import hub
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.event import event
from eventlet_framework.controller.mcp_controller.mcp_state import MC_HANDSHAK


class MCPMasterHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super(MCPMasterHandler, self).__init__(*_args, **_kwargs)
        self.name = 'mcp_master_handler'
        self.controller = None
        self.connection_dict = {}

    def start(self):
        super(MCPMasterHandler, self).start()
        self.controller = MachineControlMasterController()
        return hub.spawn(self.controller)

    @observe_event(event.EventSocketConnecting, MC_HANDSHAK)
    def connecting_handler(self, ev: event.EventSocketConnecting):
        conn = ev.connection
        # save connection in this application.
        self.connection_dict[conn.address] = conn

    @observe_event(mcp_event.EventMCPHello, MC_HANDSHAK)
    def hello_handler(self, ev):
        self.logger.debug('hello ev %s', ev)

    def execute_cmd_on_remote_machine(self, machine_ip, cmd):
        conn = self.connection_dict[machine_ip]

        msg_cmd_request = conn.mcproto_parser.MCPExecuteCommandRequest(
            conn, job_id=2, command=cmd)

        conn.send_msg(msg_cmd_request)
