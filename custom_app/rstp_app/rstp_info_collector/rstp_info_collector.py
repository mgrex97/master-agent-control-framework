from abc import ABC, abstractmethod
import asyncio
from time import time
from custom_app.util.async_api_action import APIAction
from async_app_fw.lib.hub import TaskLoop, app_hub
from custom_app.api_action_app.api_action_master_handler import ReqGetAPIAction, AGENT_LOCAL

class RequestIntervalError(Exception):
    pass

class ExpiredData(Exception):
    pass

class ExpiredRoleData(ExpiredData):
    pass

class ExpiredStateData(ExpiredData):
    pass

class ExpiredPriorityData(ExpiredData):
    pass

DISCARDING = 'discarding'
LEARNING = 'learning'
FORWRADING = 'forwarding'

DISABLE = 'disable port'
ALTERNATE = 'alternate port'
BACKUP = 'backup port'
ROOT = 'root port'
DESIGNATED = 'designated port'

spawn = app_hub.spawn

class RstpInfoCollector(ABC):
    def __init__(self, api_action=None, hostname=None, agent=AGENT_LOCAL, interval=None) -> None:

        if not api_action and not hostname:
            raise ValueError('either api_action or hostname should not be None.')

        # check api_action
        if not (isinstance(api_action, APIAction) or api_action is None):
            raise ValueError('Input value api_action should be either APIAction or None.')

        self.api_action = api_action
        self.hostname = hostname
        self.agent = agent
        self._interval = interval
        self.request_task = None
        self.collect_task = {}

    async def start(self, interval=None):
        # get APIaction from 
        if self.api_action is None:
            try:
                self.api_action: APIAction = await ReqGetAPIAction.send_request(self.hostname)
            except Exception as e:
                raise e
 
        # init info of rstp role and state, priority.
        await self.init_info()

        # check request's send interval
        interval = interval or self._interval
        self.change_interval(interval)

        self.request_task = spawn(self.request_loop)

    async def init_info(self):
        await self.request_role(time())
        await self.request_state(time())
        await self.request_priority(time())
 
    async def stop(self):
        task_loop = TaskLoop(app_hub, [task for _, task in self.collect_task.items()])
        await task_loop.wait_tasks()

    async def request_loop(self):
        while True:
            current_time = time()
            spawn(self.request_role, current_time)
            spawn(self.request_state, current_time)
            spawn(self.request_priority, current_time)
            await asyncio.sleep(self._interval)

    def change_interval(self, interval):
        if interval is None or not isinstance(interval, int) or interval <= 0:
            raise RequestIntervalError()
 
        self._interval = interval

    @abstractmethod
    async def request_role(self, time):
        pass

    @abstractmethod
    async def request_state(self, time):
        pass
 
    @abstractmethod
    async def request_priority(self, time):
        pass
