import time
import asyncio
import logging
import traceback
from pprintpp import pprint
from pyshark.packet.packet import Packet
from async_app_fw.event.event import EventBase, EventReplyBase, EventRequestBase

from async_app_fw.lib import hub
from async_app_fw.lib.hub import app_hub
from custom_app.job_app.job_util.job_class import JOB_RUNNING, JOB_STOPED, REMOTE_MATER
from custom_app.job_app.job_util import JobRequest, JobTshark
from custom_app.job_app.job_util.api_action_module import SwitchApiInfoProducer
from custom_app.job_app.job_util.job_event import JobEventRequestOutput, JobEventStateChange
from custom_app.job_app.job_util.job_handler import config_job_observe, observe_job_event
from custom_app.job_app.job_master_handler import JOB_CREATE_FAIL, EventJobManagerDelete, EventJobManagerReady, ReplyJobCreate, RequestJobCreate
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_STABLE
from custom_app.job_app.job_util.job_request_handler import config_observe_job_request_output, observe_request_output
from custom_app.topology_discovery_app.topology_item import HostState, Link, LinkState, Port, PortDataState, PortState, Switch


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


class EventLinkDelete(EventBase):
    def __init__(self, link):
        super().__init__()
        self.link = link


class EventLinkAdd(EventBase):
    def __init__(self, link):
        super().__init__()
        self.link = link


lldp_url = 'lldp/neighbors/status'
ports_url = 'ports'


def print_lldp_request_info(request_info, res):
    print(
        f'********** Get LLDP neighbors status from {request_info["host_name"]} ***********')
    for tmp in res['result']:
        port_num = int(tmp['key'])
        neighbor = tmp['val']
        print(
            f'{request_info["host_name"]}:{port_num} connect to {neighbor["SystemName"]}:{neighbor["PortIdSubtype"]}')
    print('**********************')


class TopologyDiscovery(BaseApp):

    TIMEOUT_CHECK_PERIOD = 2
    LINK_TIMEOUT = 5

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.job_managers = {}
        self.conn_map = {}
        self.job_queue = hub.Queue()
        self.sw_ip_to_info = {
            '10.88.0.11': {
                'address': '10.88.0.11',
                'device_name': 'QSW-M408-sw1',
                'mac': '24:5e:be:6b:31:f7',
                'username': 'admin',
                'password': 'qnap1234',
                'job': None,
                'lldp_task': None
            }, '10.88.0.12': {
                'address': '10.88.0.12',
                'device_name': 'QSW-M408-sw2',
                'mac': '24:5e:be:6b:31:9a',
                'username': 'admin',
                'password': 'qnap1234',
                'job': None,
                'lldp_task': None
            }, '10.88.0.13': {
                'address': '10.88.0.13',
                'device_name': 'QSW-M1208-sw3',
                'mac': '24:5e:be:6a:e9:56',
                'username': 'admin',
                'password': 'qnap1234',
                'job': None,
                'lldp_task': None
            }, '10.88.0.14': {
                'address': '10.88.0.14',
                'device_name': 'QSW-M408-sw4',
                'mac': '24:5e:be:6b:31:b8',
                'username': 'admin',
                'password': 'qnap1234',
                'job': None,
                'lldp_task': None
            }
        }
        self.agent_task_mapping = {
            '169.254.0.1': {
                'sw_ip': '10.88.0.11',
                'job_request': None,
                'job_tshark': None,
            },
            # '169.254.0.2': {
            '127.0.0.1': {
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
        self.job_to_info = {}

        # topology item
        self.switches = {}
        self.port_state = {}          # datapath_id => ports
        self.ports = PortDataState()  # Port class -> PortData class
        self.links = LinkState()      # Link class -> timestamp
        self.hosts = HostState()      # mac address -> Host class list

        for address, info in self.sw_ip_to_info.items():
            info['url_producer'] = SwitchApiInfoProducer(
                'v1', address, info['device_name'], info['username'], info['password'])

    def start(self):
        task = super().start()
        return task

    @observe_job_event(JobEventStateChange, JOB_RUNNING)
    def request_job_running_handler(self, ev: JobEventStateChange):
        job = ev.job
        info = self.job_to_info.get(job, None)
        if info is None:
            LOG.error('Job not in job_to_info')

        if ev.current == JOB_RUNNING:
            # get ports data
            job.push_request_data_to_queue('get', ports_url)

    @observe_request_output(JobEventRequestOutput, ports_url)
    def ports_request_handler(self, ev: JobEventRequestOutput):
        request_info = ev.request_info
        res = ev.res

        if request_info['url'] != ports_url:
            LOG.error(f'URL not correct {request_info["url"]} != {ports_url}')
            return

        address = request_info['host_ip']
        LOG.info(f'Get Switch Port info from {address}.')

        sw_info = self.sw_ip_to_info.get(address)
        sw_info['job'].set_output_method(self.lldp_request_handler)

        switch = self.switches.pop(address, None)

        if switch is not None:
            del switch

        switch = Switch(
            address, sw_info['mac'], sw_info['device_name'])

        self._register_switch(switch)

        for port_info in res['result']:
            port_no = int(port_info['key'])
            port = Port(address, port_no, switch.hw_addr, port_info)
            self._register_port(address, port)

        # create task to send lldp neighbor request
        if sw_info.get('lldp_task', None) is None:
            sw_info['lldp_task'] = app_hub.spawn(
                self.request_lldp_neighbors, sw_info['job'], self.TIMEOUT_CHECK_PERIOD)

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

    @observe_request_output(JobEventRequestOutput, lldp_url)
    def lldp_request_handler(self, ev: JobEventRequestOutput):
        request_info = ev.request_info
        res = ev.res

        if res['error_code'] != 200:
            return

        sw_address = request_info['host_ip']
        for tmp in res['result']:
            port_num = int(tmp['key'])
            neighbor = tmp['val']
            src = Port(sw_address, port_num,
                       self.sw_ip_to_info[sw_address]['mac'])
            neighbor_address = neighbor['ManagementAddress']
            dst = Port(neighbor_address, port_num,
                       self.sw_ip_to_info[neighbor_address]['mac'])
            self._link_update(src, dst)

    @observe_event(ReplyJobCreate, MC_STABLE)
    def job_create_handler(self, rep: ReplyJobCreate):
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

        if address in self.agent_task_mapping:
            self.create_job_request_sw(address)
            self.create_job_tshark_lldp(address)

    def create_job_request_sw(self, address):
        sw_ip = self.agent_task_mapping[address]['sw_ip']
        sw_info = self.sw_ip_to_info[sw_ip]
        url_producer: SwitchApiInfoProducer = sw_info['url_producer']

        # prepare request_option
        request_opt = dict({
            'running_times': 1000,
            'retry_mode': False,
            'host_ip': sw_ip,
            'login_info': url_producer.get_login_info(),
            'head_url': url_producer.get_instance_url()
        })

        # create request job.
        job = JobRequest(
            request_opt, remote_mode=True, remote_role=REMOTE_MATER)
        job.set_output_method(self.ports_request_handler)
        self.agent_task_mapping[address]['job_request'] = job
        req = RequestJobCreate(job, address, stamp='request')
        self.send_event(req.dst, req, MC_STABLE)

        config_job_observe(self.request_job_running_handler,
                           JobEventStateChange, job)
        config_observe_job_request_output(
            self, JobEventRequestOutput, job)

        sw_info['job'] = job
        self.job_to_info = {
            job: {
                'type': 'request',
                'sw_info': sw_info
            }
        }

    def create_job_tshark_lldp(self, address):
        lldp_tshark_opt = dict({
            'interface': 'en9',
            'bpf_filter': 'ether proto 0x88cc',
            'use_json': True
        })
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

    async def request_lldp_neighbors(self, req_job: JobRequest, interval=2):
        while req_job.state == JOB_RUNNING:
            await asyncio.sleep(interval)
            req_job.push_request_data_to_queue('get', lldp_url)

    # topology feature
    def _register_switch(self, switch: Switch):
        self.switches.setdefault(switch.mgr_addr, switch)

    def _unregister_switch(self, switch: Switch):
        return self.switches.pop(switch.mgr_addr, None)

    def _register_port(self, switch_mgr_address, port):
        if (switch := self.switches.get(switch_mgr_address, None)) is None:
            return

        port_state = self.port_state.setdefault(
            switch_mgr_address, PortState())
        port_state.add(port.port_no, port)
        switch.add_port(port.port_no, port)

    def _register_ports(self, switch_mgr_address, ports):
        if (switch := self.switches.get(switch_mgr_address, None)) is None:
            return

        port_state = self.port_state.setdefault(
            switch_mgr_address, PortState())
        for port in ports:
            port_state.add(port.port_no, port)
            switch.add_port(port.port_no, port)

    def _get_switch(self, switch_mgr_address):
        if (switch := self.switches.get(switch_mgr_address, None)) is not None:
            return None

        return switch

    def _get_port(self, switch_mgr_address, port_no):
        switch = self._get_switch(switch_mgr_address)
        if switch is not None:
            return switch.ports.get(port_no, None)

    def _link_down(self, port):
        try:
            dsts, rev_link_dsts = self.links.port_deleted(port)
        except KeyError:
            # LOG.debug('key error. src=%s, dst=%s',
            #           port, self.links.get_peer(port))
            return
        for dst in dsts:
            link = Link(port, dst)
            self.send_event_to_observers(EventLinkDelete(link))
        for rev_link_dst in rev_link_dsts:
            rev_link = Link(rev_link_dst, port)
            self.send_event_to_observers(EventLinkDelete(rev_link))
            self.ports.move_front(rev_link_dst)

    def _is_edge_port(self, port):
        for link in self.links:
            if port == link.src or port == link.dst:
                return False

        return True

    def _link_update(self, src, dst):
        link = Link(src, dst)

        if link not in self.links:
            self.send_event_to_observers(EventLinkAdd(link))

            # remove hosts if it's not attached to edge port
            host_to_del = []
            for host in self.hosts.values():
                if not self._is_edge_port(host.port):
                    host_to_del.append(host.mac)

            for host_mac in host_to_del:
                del self.hosts[host_mac]

        if not self.links.update_link(src, dst):
            # reverse link is not detected yet.
            # So schedule the check early because it's very likely it's up
            self.ports.move_front(dst)

    async def link_loop(self):
        while self.is_active:
            now = time.time()
            deleted = []
            for (link, timestamp) in self.links.items():
                # LOG.debug('%s timestamp %d (now %d)', link, timestamp, now)
                if timestamp + self.LINK_TIMEOUT < now:
                    deleted.append(link)

            for link in deleted:
                self.links.link_down(link)
                # LOG.debug('delete %s', link)
                self.send_event_to_observers(EventLinkDelete(link))

                dst = link.dst
                rev_link = Link(dst, link.src)
                if rev_link not in deleted:
                    # It is very likely that the reverse link is also
                    # disconnected. Check it early.
                    expire = now - self.LINK_TIMEOUT
                    self.links.rev_link_set_timestamp(rev_link, expire)
                    if dst in self.ports:
                        self.ports.move_front(dst)

            await asyncio.sleep(self.TIMEOUT_CHECK_PERIOD)
