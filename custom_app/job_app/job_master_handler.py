import asyncio
import logging
from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from async_app_fw.event.event import EventBase, EventReplyBase, EventRequestBase
from async_app_fw.lib import hub
from async_app_fw.protocol.mcp.mcp_parser_v_1_0 import MCPJobStateChange
from custom_app.job_app.job_util.job_class import JOB_DELETE, JOB_FAIELD, Job, REMOTE_MATER
from custom_app.job_app.job_util.job_subprocess import JobCommand
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_STABLE
from custom_app.job_app.job_manager import JobManager

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']

LOG = logging.getLogger('custom_app.job_app.jog_master_handler')

APP_NAME = 'job_master_handler'

JOB_CREATE_SUCCESS = 0
JOB_CREATE_FAIL = 1

JOB_CREATE_LOCAL = 'local'


class EventJobManagerReady(EventBase):
    def __init__(self, job_app, address=JOB_CREATE_LOCAL):
        super(EventJobManagerReady, self).__init__()
        self.address = address
        self.job_app = job_app


class EventJobManagerDelete(EventBase):
    def __init__(self, address):
        super(EventJobManagerDelete, self).__init__()
        self.address = address


class RequestJobCreate(EventRequestBase):
    def __init__(self, job, address=None, timeout=None, stamp=''):
        super().__init__()
        self.dst = APP_NAME
        self.job = job
        self.address = address
        self.timeout = timeout
        self.stamp = stamp


class ReplyJobCreate(EventReplyBase):
    def __init__(self, dst, job, address, create_result, stamp):
        super().__init__(dst)
        self.create_result = create_result
        self.job = job
        self.address = address
        self.stamp = stamp

    @classmethod
    def create_by_request(cls, req, create_result):
        return cls(req.src, req.job, req.address, create_result, req.stamp)


class CreateRequestManager():
    def __init__(self, address) -> None:
        self.address = address
        self.xid_to_job = {}

    def add_request(self, xid, req):
        # check duplicate xid
        assert xid not in self.xid_to_job
        self.xid_to_job[xid] = req

    def pop_request(self, xid):
        return self.xid_to_job.pop(xid, None)


class JobMasterHandler(BaseApp):
    _EVENTS = [EventJobManagerDelete, EventJobManagerReady, ReplyJobCreate]

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.job_managers = {}
        self.job_managers[JOB_CREATE_LOCAL] = JobManager()
        self.create_request = {}
        self.conn_map = {}
        self.job_queue = hub.Queue()

    def start(self):
        task = super().start()
        return task

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def agent_join(self, ev):
        conn = ev.connection
        address = conn.address[0]
        conn_id = conn.id

        LOG.info(f'Create Job Manager {address}')

        self.job_managers[conn_id] = JobManager(
            ev.connection)
        self.create_request[conn_id] = CreateRequestManager(address)

        if address in self.conn_map:
            LOG.warning(f'There is duplicate address. {address}')

        self.conn_map[address] = conn_id

        self.send_event_to_observers(
            EventJobManagerReady(self, address), MC_STABLE)

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def agent_leave(self, ev):
        conn = ev.connection
        conn_id = conn.id
        address = conn.address[0]
        LOG.info(f'Delete Job Manager {address}.')

        if job_manager := self.job_managers.pop(conn_id, None):
            del job_manager
        if create_request := self.create_request.pop(conn_id, None):
            for _, req in create_request.xid_to_job.items():
                self.send_event(
                    req.src, ReplyJobCreate.create_by_request(req, JOB_CREATE_FAIL))

            del create_request

        self.send_event_to_observers(
            EventJobManagerDelete(address), MC_STABLE)

    def agent_machine_leave(self, ev):
        del self.job_managers[ev.connection.id]

    @observe_event(mcp_event.EventMCPJobCreateReply, MC_STABLE)
    def job_create_reply_handler(self, ev):
        LOG.info(f'Get Job create reply')

        msg = ev.msg
        xid = msg.xid
        conn: MachineConnection = ev.msg.connection
        id = conn.id
        self.job_managers[conn.id].job_id_async(xid, msg.job_id)

        # msg = conn.mcproto_parser.MCPJobACK(conn, job_id)
        # conn.send_msg(msg)
        if conn.id not in self.create_request:
            LOG.warning(
                f'Connection ID {id} not exist in dict create_request.')
            return

        if (req := self.create_request[conn.id].pop_request(xid)) is None:
            LOG.warning(
                f'xid {xid} not exist in CreateRequestManager.')
            return

        rep = ReplyJobCreate.create_by_request(req, JOB_CREATE_SUCCESS)
        self.send_event(req.src, rep)

    @observe_event(mcp_event.EventMCPJobDeleteReply, MC_STABLE)
    def job_deleted_handler(self, ev):
        conn = ev.msg.connection
        job_id = ev.msg.job_id
        self.job_managers[conn.id].del_job(job_id)

    @observe_event(mcp_event.EventMCPJobOutput, MC_STABLE)
    def job_output_handler(self, ev):
        conn_id = ev.msg.connection.id
        job_id = ev.msg.job_id
        job: Job = self.job_managers[conn_id].get_job(job_id)
        if job is None:
            LOG.warning(f'Job Output Error: {ev.msg.job_id} not exist.')
            return
        job.get_remote_output(ev.msg.state, ev.msg.info)

    @observe_event(mcp_event.EventMCPJobStateChange, MC_STABLE)
    def job_state_change_hanlder(self, ev):
        msg: MCPJobStateChange = ev.msg
        job: Job = self.job_managers[msg.connection.id].get_job(ev.msg.job_id)
        if job is None:
            LOG.warning(f'Job State Change Error: {ev.msg.job_id} not exist.')
            return
        job.remote_change_state(msg.before, msg.after, msg.info)

    def exe_cmd_on_agent(self, address, command):
        assert address in self.job_managers

        job_manager: JobManager = self.job_managers[address]
        job = JobCommand(job_manager.connection, command)

        msg = job_manager.connection.mcproto_parser.MCPJobCreateRequest(
            job_manager.connection, timeout=10, job_info=job.job_info_serialize())

        # msg's xid is None, give new xid to msg.
        job_manager.connection.set_xid(msg)
        job_manager.add_request(xid=msg.xid, job_obj=job)
        job_manager.connection.send_msg(msg)

    def clear_job(self, address):
        if address not in self.conn_map:
            return True

        conn_id = self.conn_map[address]
        job_manager: JobManager = self.job_managers[conn_id]
        conn = job_manager.connection
        # if job not exist in job manager.

        msg = conn.mcproto_parser.MCPJobDeleteAll(
            conn)
        conn.send_msg(msg)
        job_manager.delete_all_job()

    def delete_job(self, job: Job, address):
        if address not in self.conn_map:
            job.change_state(JOB_DELETE)
            return True

        conn_id = self.conn_map[address]
        job_manager: JobManager = self.job_managers[conn_id]
        # if job not exist in job manager.
        res = job_manager.get_job(job.id)
        if res is None:
            job.change_state(JOB_DELETE)
            return True

        msg = job.connection.mcproto_parser.MCPJobDeleteRequest(
            job.connection, job.id)
        job.connection.set_xid(msg)
        job.connection.send_msg(msg)

        return False

    @observe_event(RequestJobCreate, MC_STABLE)
    def create_job(self, req):
        job: Job = req.job
        address = req.address

        if (conn_id := self.conn_map.get(address, None)) is None:
            LOG.info(f'Client {address} not exist.')
            job.change_state(JOB_FAIELD)
            rep = ReplyJobCreate.create_by_request(req, JOB_CREATE_FAIL)
            self.send_event(req.src, rep)

            return

        if address == JOB_CREATE_LOCAL:
            self.job_managers[JOB_CREATE_LOCAL].add_job_with_new_id(job)
        else:
            # check ip
            job.remote_mode = True
            job.remote_role = REMOTE_MATER
            job_manager: JobManager = self.job_managers[conn_id]
            msg = job_manager.connection.mcproto_parser.MCPJobCreateRequest(
                job_manager.connection, timeout=job.timeout, job_info=job.job_info_serialize())

            # msg's xid is None, give new xid to msg.
            job_manager.connection.set_xid(msg)
            job_manager.add_request(xid=msg.xid, job_obj=job)
            job_manager.connection.send_msg(msg)

            self.create_request[conn_id].add_request(msg.xid, req)
