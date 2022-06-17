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

import asyncio
import contextlib
import logging
import traceback
from asyncio import CancelledError, StreamReader, StreamWriter
from socket import IPPROTO_TCP
from socket import TCP_NODELAY
from socket import SHUT_WR
from eventlet_framework.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_HANDSHAK
from eventlet_framework.controller.mcp_controller.mcp_controller import MachineConnection

# from ryu import cfg
from eventlet_framework.lib import hub

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(
    'eventlent_framework.controller.tshark.tshark_controller')


class MachineControlAgentController(object):
    def __init__(self, host='127.0.0.1', port=7930):
        self.host = host
        self.port = port

    async def attempt_connecting_loop(self, interval=None):
        agent = hub.StreamClient(
            addr=(self.host, self.port))
        await agent.connect_loop(machine_connection_factory, interval=interval)


class AgentConnection(MachineConnection):
    def __init__(self, reader, writer, mcp_brick_name='mcp_agent_handler'):
        super(AgentConnection, self).__init__(
            reader, writer, mcp_brick_name=mcp_brick_name)


async def machine_connection_factory(reader: StreamReader, writer: StreamWriter):
    socket = writer.get_extra_info('socket')
    address = socket.getpeername()
    LOG.info('connected socket: address:%s port:%s',
             *address)

    with contextlib.closing(AgentConnection(reader, writer)) as machine_connection:
        try:
            serve_task = hub.app_hub.spawn(machine_connection.serve)
            await serve_task
        except Exception as e:
            # Something went wrong.
            # Especially malicious switch can send malformed packet,
            # the parser raise exception.
            # Can we do anything more graceful?
            # print(e)
            print(traceback.format_exc())
            """
            if datapath.id is None:
                dpid_str = "%s" % datapath.id
            else:
                dpid_str = dpid_to_str(datapath.id)
            LOG.error("Error in the datapath %s from %s", dpid_str, address)
            raise
            """
        finally:
            LOG.info(f'Disconnect to {address}')
            serve_task.cancel()
            machine_connection.set_state(MC_DISCONNECT)
