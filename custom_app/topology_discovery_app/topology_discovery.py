import asyncio
import logging
import traceback
from pyshark.packet.packet import Packet

from async_app_fw.lib import hub
from async_app_fw.lib.hub import app_hub
from custom_app.job_app.job_util.job_class import JOB_CREATE, JOB_CREATED, JOB_DELETE, JOB_FAIELD, REMOTE_MATER, STATE_MAPPING, Job
from custom_app.job_app.job_util import JobRequest, JobTshark
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
                'device_name': 'QSW-M408-sw1',
                'sw_ip': '10.88.0.11',
                'job_request': None,
                'running': False,
                'job_tshark': None,
                'tshark_running': False,
            },
            '169.254.0.2': {
                'device_name': 'QSW-M408-sw2',
                'sw_ip': '10.88.0.12',
                'job_request': None,
                'running': False,
                'job_tshark': None,
                'tshark_running': False,
            },
            '169.254.0.3': {
                'device_name': 'QSW-M1208-sw3',
                'sw_ip': '10.88.0.13',
                'job_request': None,
                'running': False,
                'job_tshark': None,
                'tshark_running': False
            },
            '169.254.0.4': {
                'device_name': 'QSW-M408-sw4',
                'sw_ip': '10.88.0.14',
                'job_request': None,
                'running': False,
                'job_tshark': None,
                'tshark_running': False
            }
        }
        self.mac_to_sw_name = {
            '24:5e:be:6b:31:f7': 'QSW-M408-sw1',
            '24:5e:be:6b:31:9a': 'QSW-M408-sw2',
            '24:5e:be:6a:e9:56': 'QSW-M1208-sw3',
            '24:5e:be:6b:31:b8': 'QSW-M408-sw4'
        }
        self.api_action = SwitchAPIAction()

    def start(self):
        task = super().start()
        self.job_request_checker_task = app_hub.spawn(self.job_checker)
        return task

    async def job_checker(self):
        try:
            while True:
                for address, sw_info in self.agent_sw_ip_mapping.items():
                    sw_name = sw_info['device_name']
                    if (job_request := sw_info['job_request']) is not None:
                        if job_request.state == JOB_CREATE:
                            # LOG.info(
                            # f'Client <{address}> Ready to run. Target SW: {sw_name}')
                            if sw_info['running'] == False:
                                job_request.run()
                                sw_info['running'] = True

                    if (job_tshark := sw_info['job_tshark']) is not None:
                        if job_tshark.state == JOB_CREATE:
                            # LOG.info(
                            # f'Client <{address}> Ready to run. Target SW: {sw_name}')
                            if sw_info['tshark_running'] == False:
                                job_tshark.run()
                                sw_info['tshark_running'] = True

                await asyncio.sleep(5)
        except Exception as e:
            print(traceback.format_exc())

    def lldp_tshark_hadler(self, tshark_info, packet: Packet):
        try:
            client_address = tshark_info['address']
            mac = packet.lldp.get('chassis_subtype_=_mac_address,_id').mac
            interface_id = packet.lldp.get(
                'port_subtype_=_interface_alias,_id').id
            print(
                f'Client {client_address} connection to: {self.mac_to_sw_name[mac]} port {interface_id}')
        except Exception:
            LOG.warning(
                f'client tshark parse error\n {traceback.format_exc()}')

    def lldp_request_handler(self, request_info, res):
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

        lldp_request_opt = dict({
            'running_times': 1000,
            'retry_mode': True,
            'retry_data': {
                'type': 'get',
                'url': None
            },
            'host_ip': None,
            'login_info': None
        })

        lldp_tshark_opt = dict({
            'interface': 'eth0',
            'bpf_filter': 'ether proto 0x88cc',
            'use_json': True
        })

        if address in self.agent_sw_ip_mapping:
            sw_info = self.agent_sw_ip_mapping[address]
            self.api_action.host_ip = sw_info['sw_ip']
            self.api_action.hostname = sw_info['device_name']
            # prepare request_info
            lldp_request_opt['host_ip'] = sw_info['sw_ip']
            lldp_request_opt['login_info'] = self.api_action.get_login_info()
            lldp_request_opt['retry_data']['url'] = self.api_action.get_api_url_with_path(
                url)

            # create request job.
            job = JobRequest(
                lldp_request_opt, remote_mode=True, remote_role=REMOTE_MATER)
            job.set_output_method(self.lldp_request_handler)
            self.agent_sw_ip_mapping[address]['job_request'] = job
            # job_app.install_job(job, address)

            # create tshark job
            job_t = JobTshark(tshark_options=lldp_tshark_opt,
                              remote_mode=True, remote_role=REMOTE_MATER)
            job_t.set_output_method(self.lldp_tshark_hadler)
            self.agent_sw_ip_mapping[address]['job_tshark'] = job_t
            job_app.install_job(job_t, address)

    @observe_event(EventJobManagerDelete, MC_STABLE)
    def job_manager_delete(self, ev):
        address = ev.address

        self.agent_sw_ip_mapping[address]['job_request'] = None
        self.agent_sw_ip_mapping[address]['running'] = False
