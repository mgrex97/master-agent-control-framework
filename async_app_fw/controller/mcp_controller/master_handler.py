import asyncio
from collections import defaultdict
import logging
from typing import Dict, Set
from async_app_fw.controller.handler import observe_event, observe_event_from_self
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from async_app_fw.event import event
from async_app_fw.controller.mcp_controller.mcp_state import \
    MC_DISCONNECT, MC_FEATURE, MC_HANDSHAK, MC_STABLE
from async_app_fw.controller.mcp_controller.master_controller import MachineControlMasterController
from async_app_fw.event.mcp_event import mcp_event

from .master_lib.constant import APP_NAME
from .master_lib.event import ReqGetAgentConnection, ReqWaitAgentsConnect
from .exception import AgentIsNotExist

spawn = app_hub.spawn

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
        self.connection_event: Dict[str, asyncio.Event] = defaultdict(lambda: asyncio.Event())
        self.connection_check_task: Set[asyncio.Task] = set()

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

        # make sure there is not connection check task
        coros = []
        connection_check_task = self.connection_check_task
        self.connection_check_task.clear()

        for task in connection_check_task:
            coros.append(task.cancel())

        await asyncio.gather(*coros)

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

        if conn.address[0] in self.connection_event:
            self.connection_event[conn.address[0]].set()

    @observe_event(ReqGetAgentConnection)
    def get_agent_connection_handler(self, ev: ReqGetAgentConnection):
        result = self.ip_to_connection.get(ev.agent, None) or AgentIsNotExist(ev.agent)
        ev.push_reply(result)

    async def check_connection(self, ev: ReqWaitAgentsConnect):
        agents:list = ev.agents
        ip_to_connection = self.ip_to_connection

        remaining_agents = []

        event_dict = {}
        wait_coros = []

        for agent in agents:
            # check the target agent exist or not.
            if agent in ip_to_connection:
                continue

            # create task to wait conenction event been set.
            remaining_agents.append(agent)
            conn_event: asyncio.Event = self.connection_event[agent]
            if conn_event.is_set():
                conn_event.clear()
            event_dict[agent] = conn_event
            wait_coros.append(conn_event.wait())

        if len(remaining_agents) == 0:
            ev.push_reply(True)
            return

        try:
            gather_tasks = asyncio.gather(*wait_coros)
            await asyncio.wait_for(gather_tasks, timeout=ev.wait_connection_timeout)
            ev.push_reply(True)
        except asyncio.TimeoutError:
            remaining_agents.clear()

            for agent, conn_event in event_dict.items():
                if conn_event.is_set():
                    continue
                remaining_agents.append(agent)

            ev.push_reply(Exception(f"Agent not connect, {remaining_agents}"))

        self.connection_check_task.remove(asyncio.current_task())

    @observe_event(ReqWaitAgentsConnect)
    def wait_agents_handler(self, ev: ReqWaitAgentsConnect):
        self.connection_check_task.add(spawn(self.check_connection, ev))