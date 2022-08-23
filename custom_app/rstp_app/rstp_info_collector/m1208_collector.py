import logging
import aiohttp
from custom_app.api_action_app.api_action_master_handler import AGENT_LOCAL
from custom_app.rstp_app.rstp_info_collector.rstp_info_collector import ExpiredRoleData, ExpiredStateData, RstpInfoCollector, \
    DISCARDING, LEARNING, FORWRADING, DISABLE, ALTERNATE, BACKUP, ROOT, DESIGNATED


#@version register
#@modal register
#@info type register
class M1208RstpCollector(RstpInfoCollector):
    rstp_state_mapping = {
        0: DISCARDING,
        1: LEARNING,
        2: FORWRADING
    }
    
    rstp_role_mapping = {
        0: DISABLE,
        1: ALTERNATE,
        2: BACKUP,
        3: ROOT,
        4: DESIGNATED,
    }

    def __init__(self, api_action=None, hostname=None, agent=AGENT_LOCAL, interval=None) -> None:
        super().__init__(api_action, hostname, agent, interval)
        self.role = {}
        self.role_time = 0
        self.state = {}
        self.state_time = 0
        self.priority = 0
        self.priority_time = 0

    async def request_role(self, time):
        resp: aiohttp.ClientResponse = await self.api_action.get('rstp/interface/role', agent=self.agent)

        if self.role_time > time:
            raise ExpiredRoleData()

        self.role_time = time
        
        role: dict = self.role

        for info in resp.json['result']:
            role[info['key']] = self.rstp_role_mapping[info['val']]

    async def request_state(self, time):
        resp: aiohttp.ClientResponse = await self.api_action.get('rstp/interface/state', agent=self.agent)

        if self.role_time > time:
            raise ExpiredStateData()
        
        self.state_time = time

        state : dict = self.state
        for info in resp.json['result']:
            state[info['key']] = self.rstp_state_mapping[info['val']]

    async def request_priority(self, time):
        resp: aiohttp.ClientResponse = await self.api_action.get('rstp/priority', agent=self.agent)

        if self.priority_time > time:
            raise ExpiredStateData()
 
        self.priority_time = time
        self.priority = resp.json['result'][0]['val']

    def get_port_info(self, port_num):
        info = {}

        info['role'] = self.role.get(port_num, None)
        info['state'] = self.state.get(port_num, None)
        info['priority'] = self.priority

        return info