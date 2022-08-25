from abc import ABC, abstractmethod
import asyncio
from time import time
from custom_app.util.async_api_action import APIAction
from async_app_fw.lib.hub import TaskLoop, app_hub
from custom_app.api_action_app.api_action_master_handler import ReqGetAPIAction, AGENT_LOCAL

class GetInfoIntervalError(Exception):
    pass

class ExpiredData(Exception):
    pass

spawn = app_hub.spawn

INFO_REQEUEST_DEFAULT_TIMEOUT = 10

class InfoCollector(ABC):
    def __init__(self, api_action=None, hostname=None, agent=AGENT_LOCAL, interval=None, request_timeout=INFO_REQEUEST_DEFAULT_TIMEOUT) -> None:

        if not api_action and not hostname:
            raise ValueError('either api_action or hostname should not be None.')

        # check api_action
        if not (isinstance(api_action, APIAction) or api_action is None):
            raise ValueError('Input value api_action should be either APIAction or None.')

        self.api_action = api_action
        self.hostname = hostname
        self.agent = agent
        self._interval = interval
        self._requset_timeout = request_timeout
        self._info_collect_task = None
        self.collect_task = []

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

        self._info_collect_task = spawn(self.info_collect_loop)

    @abstractmethod
    async def init_info(self):
        pass
 
    async def stop(self):
        if self._info_collect_task is not None and not self._info_collect_task.done():
            self.collect_task.append(self._info_collect_task)

        if len(self.collect_task) > 0:
            for task in self.collect_task:
                if not task.done():
                    task.cancel()

            await asyncio.wait(self.collect_task)

    async def info_collect_loop(self):
        while True:
            await self.get_info()
            await asyncio.sleep(self._interval)

    @abstractmethod
    async def get_info(self):
        pass

    def change_interval(self, interval):
        if interval is None or not isinstance(interval, int) or interval <= 0:
            raise GetInfoIntervalError()
 
        self._interval = interval
