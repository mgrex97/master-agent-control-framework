import asyncio
import logging
import traceback
from pyshark.packet.packet import Packet
from async_app_fw.event.event import EventBase, EventReplyBase, EventRequestBase

from async_app_fw.lib import hub
from async_app_fw.lib.hub import app_hub
from custom_app.job_app.job_util.job_class import REMOTE_MATER
from custom_app.job_app.job_util import JobRequest, JobTshark
from custom_app.job_app.job_util.api_action_module import SwitchAPIAction
from custom_app.job_app.job_master_handler import JOB_CREATE_FAIL, EventJobManagerDelete, EventJobManagerReady, ReplyJobCreate, RequestJobCreate
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_STABLE


_REQUIRED_APP = [
    'custom_app.job_app.job_master_handler']

LOG = logging.getLogger('custom_app.topology_discovery')

APP_NAME = 'topology_discovery'


class RequestTopologyInfo(EventRequestBase):
    def __init__(self):
        super().__init__()
        self.dst = APP_NAME


class ReplyTopologyInfo(EventReplyBase):
    def __init__(self, dst, topology_info):
        super().__init__(dst)
        self.topology_info = topology_info

    @classmethod
    def create_by_request(cls, req, topology_info):
        return cls(req.dst, topology_info)


class EventTopologyChange(EventBase):
    # not implement yet.
    def __init__(self) -> None:
        super().__init__()


class TopologyDiscovery(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.job_managers = {}
        self.conn_map = {}
        self.job_queue = hub.Queue()
        self.switches = {}
        self.sw_ip_to_info = {
            '10.88.0.11': {
                'device_name': 'QSW-M408-sw1',
                'mac': '24:5e:be:6b:31:f7',
            }, '10.88.0.12': {
                'device_name': 'QSW-M408-sw2',
                'mac': '24:5e:be:6b:31:9a',
            }, '10.88.0.13': {
                'device_name': 'QSW-M1208-sw3',
                'mac': '24:5e:be:6a:e9:56',
            }, '10.88.0.14': {
                'device_name': 'QSW-M408-sw4',
                'mac': '24:5e:be:6b:31:b8',
            }
        }
        self.agent_task_mapping = {
            '169.254.0.1': {
                'sw_ip': '10.88.0.11',
                'job_request': None,
                'job_tshark': None,
            },
            '169.254.0.2': {
                'sw_ip': '10.88.0.12',
                'job_request': None,
                'job_tshark': None,
            },
            '169.254.0.3': {
                'sw_ip': '10.88.0.13',
                'job_request': None,
                'job_tshark': None,
            },
            '169.254.0.4': {
                'sw_ip': '10.88.0.14',
                'job_request': None,
                'job_tshark': None,
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
        return task

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
                f'{request_info["host_name"]}:{port_num} connect to {neighbor["SystemName"]}:{neighbor["PortIdSubtype"]}')
        print('**********************')

    @observe_event(ReplyJobCreate, MC_STABLE)
    def job_create_handler(self, rep):
        if rep.create_result == JOB_CREATE_FAIL:
            # do something if job create failed.
            pass

        job = rep.job
        address = rep.address

        LOG.info(
            f'Client {address} Job {rep.stamp} create success. Start running...')
        job.run()

    @observe_event(EventJobManagerReady, MC_STABLE)
    def job_manager_ready(self, ev):
        address = ev.address
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

        if address in self.agent_task_mapping:
            target_sw_ip = self.agent_task_mapping[address]['sw_ip']
            sw_info = self.sw_ip_to_info[target_sw_ip]
            self.api_action.host_ip = target_sw_ip
            self.api_action.hostname = sw_info['device_name']
            # prepare request_info
            lldp_request_opt['host_ip'] = target_sw_ip
            lldp_request_opt['login_info'] = self.api_action.get_login_info()
            lldp_request_opt['retry_data']['url'] = self.api_action.get_api_url_with_path(
                url)

            # create request job.
            job_r = JobRequest(
                lldp_request_opt, remote_mode=True, remote_role=REMOTE_MATER)
            job_r.set_output_method(self.lldp_request_handler)
            self.agent_task_mapping[address]['job_request'] = job_r
            req = RequestJobCreate(job_r, address, stamp='request')
            self.send_event(req.dst, req, MC_STABLE)

            # create tshark job
            job_t = JobTshark(tshark_options=lldp_tshark_opt,
                              remote_mode=True, remote_role=REMOTE_MATER)
            job_t.set_output_method(self.lldp_tshark_hadler)
            self.agent_task_mapping[address]['job_tshark'] = job_t
            req = RequestJobCreate(job_t, address, stamp='tshark')
            self.send_event(req.dst, req, MC_STABLE)

    @observe_event(EventJobManagerDelete, MC_STABLE)
    def job_manager_delete(self, ev):
        address = ev.address
        if address in self.agent_task_mapping:
            agent_task = self.agent_task_mapping[address]
            agent_task['job_request'] = None
            agent_task['job_tshark'] = None
