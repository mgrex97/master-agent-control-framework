import contextlib
from eventlet_framework.controller.mcp_controller.mcp_controller import MachineConnection
from eventlet_framework.lib.hub import StreamServer
from eventlet_framework.lib import hub
from eventlet_framework.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from socket import SHUT_WR
from socket import TCP_NODELAY
from socket import IPPROTO_TCP
from socket import socket
import traceback
import logging

# from ryu import cfg

LOG = logging.getLogger(
    'eventlent_framework.controller.mcp_controller.master_controller')


class MachineControlMasterController(object):
    def __init__(self):
        self.tcp_listen_port = 7930
        self.listen_host = '169.254.0.111'
        hub.spawn(self.server_loop, self.tcp_listen_port)
        self._clients = {}

    def __call__(self):
        self.server_loop(self.tcp_listen_port)

    def server_loop(self, tshark_tcp_listen_port):
        server = StreamServer(
            (self.listen_host, tshark_tcp_listen_port), machine_connection_factory)
        server.serve_forever()


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

    def serve(self):
        if self.id == 0:
            self.id = self._get_new_machine_id()

        msg_hello = self.mcproto_parser.MCPHello(self, self.id)

        self.send_msg(msg_hello)
        return super().serve()


def machine_connection_factory(socket: socket, address):
    LOG.debug('connected socket:%s address:%s port:%s',
              socket, *socket.getpeername())
    with contextlib.closing(MasterConnection(socket, address)) as machine_connection:
        try:
            machine_connection.serve()
        except Exception as e:
            # Something went wrong.
            # Especially malicious switch can send malformed packet,
            # the parser raise exception.
            # Can we do anything more graceful?
            print(e)
            print(traceback.format_exc())
            """
            if datapath.id is None:
                dpid_str = "%s" % datapath.id
            else:
                dpid_str = dpid_to_str(datapath.id)
            LOG.error("Error in the datapath %s from %s", dpid_str, address)
            raise
            """
