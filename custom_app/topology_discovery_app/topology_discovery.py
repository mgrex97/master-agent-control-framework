import asyncio
import logging
import traceback

from async_app_fw.lib import hub
from async_app_fw.lib.hub import app_hub
from custom_app.job_app.job_util.job_class import JOB_CREATE, JOB_CREATED, JOB_DELETE, JOB_FAIELD, REMOTE_MATER, STATE_MAPPING, Job
from custom_app.job_app.job_util import JobRequest
from custom_app.job_app.job_util.api_action_module import SwitchAPIAction
from custom_app.job_app.job_master_handler import EventJobManagerDelete, JobMasterHandler, EventJobManagerReady
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_STABLE


_REQUIRED_APP = [
    'custom_app.job_app.job_master_handler']

LOG = logging.getLogger('custom_app.topology_discovery')


class TopologyDiscovery(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'topology_discovery'
        self.job_managers = {}
        self.conn_map = {}
        self.job_queue = hub.Queue()
        self.agent_sw_ip_mapping = {
            '169.254.0.1': {
                # '127.0.0.1': {
                'device_name': 'QSW-M408-sw1',
                'sw_ip': '10.88.0.11',
                'job_request': None,
                'running': False
            },
            '169.254.0.2': {
                'device_name': 'QSW-M408-sw2',
                'sw_ip': '10.88.0.12',
                'job_request': None,
                'running': False
            },
            '169.254.0.3': {
                'device_name': 'QSW-M408-sw3',
                'sw_ip': '10.88.0.13',
                'job_request': None,
                'running': False
            },
            '169.254.0.4': {
                'device_name': 'QSW-M408-sw4',
                'sw_ip': '10.88.0.14',
                'job_request': None,
                'running': False
            }
        }
        self.sw_connecting_map = {

        }
        self.api_action = SwitchAPIAction()

    def start(self):
        task = super().start()
        self.job_request_checker_task = app_hub.spawn(self.job_request_checker)
        return task

    async def job_request_checker(self):
        try:
            while True:
                for address, sw_info in self.agent_sw_ip_mapping.items():
                    if (job_request := sw_info['job_request']) is None:
                        continue

                    sw_name = sw_info['device_name']
                    if job_request.state == JOB_CREATE:
                        # LOG.info(
                        # f'Client <{address}> Ready to run. Target SW: {sw_name}')
                        if sw_info['running'] == False:
                            job_request.run()
                            sw_info['running'] = True

                await asyncio.sleep(5)
        except Exception as e:
            print(traceback.format_exc())

    def lldp_res_handler(self, request_info, res):
        LOG.info('handle by topology discovery.')
        if res['error_code'] != 200:
            return

        print(
            f'********** Get LLDP neighbors status from {request_info["host_name"]} ***********')
        for tmp in res['result']:
            port_num = tmp['key']
            neighbor = tmp['val']
            print(
                f'{request_info["host_name"]}:{port_num} connect to {neighbor["SystemName"]:{neighbor["PortIdSubtype"]}}')
        print('**********************')

    @observe_event(EventJobManagerReady, MC_STABLE)
    def job_manager_ready(self, ev):
        address = ev.address
        job_app: JobMasterHandler = ev.job_app
        url = 'lldp/neighbors/status'

        request_info_lldp = dict({
            'running_times': 1000,
            'retry_mode': True,
            'retry_data': {
                'type': 'get',
                'url': None
            },
            'host_ip': None,
            'login_info': None
        })

        if address in self.agent_sw_ip_mapping:
            sw_info = self.agent_sw_ip_mapping[address]
            self.api_action.host_ip = sw_info['sw_ip']
            self.api_action.hostname = sw_info['device_name']
            # prepare request_info
            request_info_lldp['host_ip'] = sw_info['sw_ip']
            request_info_lldp['login_info'] = self.api_action.get_login_info()
            request_info_lldp['retry_data']['url'] = self.api_action.get_api_url_with_path(
                url)

            job = JobRequest(
                request_info_lldp, remote_mode=True, remote_role=REMOTE_MATER)
            job.set_output_method(self.lldp_res_handler)
            self.agent_sw_ip_mapping[address]['job_request'] = job
            job_app.install_job(job, address)

    @observe_event(EventJobManagerDelete, MC_STABLE)
    def job_manager_delete(self, ev):
        address = ev.address

        self.agent_sw_ip_mapping[address]['job_request'] = None
        self.agent_sw_ip_mapping[address]['running'] = False
