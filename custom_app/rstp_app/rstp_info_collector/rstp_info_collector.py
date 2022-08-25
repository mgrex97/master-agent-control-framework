from abc import ABC, abstractmethod
import asyncio
from time import time
from async_app_fw.lib.hub import TaskLoop, app_hub
from custom_app.util.info_collector import InfoCollector, ExpiredData

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

class RstpInfoCollector(InfoCollector, ABC):
    async def init_info(self):
        current_time = time()
        await self.request_role(current_time)
        await self.request_state(current_time)
        await self.request_priority(current_time)

    async def get_info(self):
        current_time = time()
        spawn(self.request_role, current_time)
        spawn(self.request_state, current_time)
        spawn(self.request_priority, current_time)

    @abstractmethod
    async def request_role(self, time):
        pass

    @abstractmethod
    async def request_state(self, time):
        pass
 
    @abstractmethod
    async def request_priority(self, time):
        pass
