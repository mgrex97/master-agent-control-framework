# Copyright (C) 2011, 2012 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011, 2012 Isaku Yamahata <yamahata at valinux co jp>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The main component of Wiresahrk controller.

- Handle connections from client

"""

import contextlib
import logging
import random
import traceback
from socket import socket
from socket import IPPROTO_TCP
from socket import TCP_NODELAY
from socket import SHUT_WR
from eventlet_framework.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from eventlet_framework.controller.mcp_controller.mcp_controller import MachineConnection

# from ryu import cfg
from eventlet_framework.lib import hub
from eventlet_framework.lib.hub import StreamClient

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(
    'eventlent_framework.controller.tshark.tshark_controller')


class MachineControlAgentController(object):
    def __init__(self):
        self.tcp_connecting_port = 7930
        self.connection_host = '169.254.0.111'
        hub.spawn(self.attempt_connecting_loop, self.tcp_connecting_port)

    def __call__(self):
        self.attempt_connecting_loop(self.tcp_connecting_port)

    def attempt_connecting_loop(self, tcp_connecting_port, interval=None):
        agent = StreamClient(
            addr=(self.connection_host, tcp_connecting_port))
        agent.connect_loop(machine_connection_factory, interval=2)


class AgentConnection(MachineConnection):
    def __init__(self, socket, address, mcp_brick_name='mcp_agent_handler'):
        super(AgentConnection, self).__init__(
            socket, address, mcp_brick_name=mcp_brick_name)


def machine_connection_factory(socket: socket, address):
    LOG.debug('connected socket:%s address:%s port:%s',
              socket, *socket.getpeername())
    with contextlib.closing(AgentConnection(socket, address)) as machine_connection:
        try:
            machine_connection.serve()

        except Exception as e:
            # Something went wrong.
            # Especially malicious switch can send malformed packet,
            # the parser raise exception.
            # Can we do anything more graceful?
            print(e)
            # print(traceback.format_exc())
            """
            if datapath.id is None:
                dpid_str = "%s" % datapath.id
            else:
                dpid_str = dpid_to_str(datapath.id)
            LOG.error("Error in the datapath %s from %s", dpid_str, address)
            raise
            """
        finally:
            machine_connection.set_state(MC_DISCONNECT)
