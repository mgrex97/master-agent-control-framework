import asyncio
import logging
from eventlet_framework.lib import hub
from eventlet_framework.lib.hub import app_hub
from custom_app.job_app.async_job_app.job_class import JOB_ASYNC, JOB_CREATE_FAILD, JOB_DELETE, JOB_RUNING, Job, JobCommand
from eventlet_framework.base.async_app_manager import BaseApp
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event
from eventlet_framework.controller.mcp_controller.async_ver.mcp_state import MC_STABLE
from custom_app.job_app.async_job_app.job_manager import JobManager

_REQUIRED_APP = [
    'eventlet_framework.controller.mcp_controller.async_ver.master_handler']

LOG = logging.getLogger('custom_app.job_app.jog_master_handler')


class JobMasterHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'job_master_handler'
        self.job_managers = {}
        self.conn_map = {}
        self.job_queue = hub.Queue()

    def start(self):
        task = super().start()
        # app_hub.spawn(self.job_consumer)
        return task

    """
    def job_consumer(self):
        while True:
            try:
                job_get = self.job_queue.get(block=False)
                LOG.info(f'Get item from Queue. Item {job_get}')
            except Empty:
                continue

            job = job_get['job']
            address = job_get['address']
            self.install_job(job, address)
    """

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def agent_join(self, ev):
        conn = ev.connection
        self.job_managers[conn.id] = JobManager(
            ev.connection)

        self.conn_map = {conn.address[0]: conn.id}

    def agent_machine_leave(self, ev):
        del self.job_managers[ev.connection.id]

    @observe_event(mcp_event.EventMCPJobCreateReply, MC_STABLE)
    def job_create_reply_handler(self, ev):
        LOG.info(f'Get Job create reply')

        msg = ev.msg
        conn = ev.msg.connection
        job_id = ev.msg.job_id
        self.job_managers[conn.id].job_id_async(msg.xid, msg.job_id)

        msg = conn.mcproto_parser.MCPJobACK(conn, job_id)
        conn.send_msg(msg)

    @observe_event(mcp_event.EventMCPJobStateInform, MC_STABLE)
    def job_state_inform_handler(self, ev):
        LOG.info('get inform')

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
        job.change_state(JOB_RUNING)
        # job.output_handler(ev.msg.job_info)

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

    def install_job(self, job: Job, address):
        try:
            conn_id = self.conn_map[address]
        except KeyError:
            LOG.info(f'Client {address} not exist.')
            job.change_state(JOB_CREATE_FAILD)
            return False

        job_manager: JobManager = self.job_managers[conn_id]
        msg = job_manager.connection.mcproto_parser.MCPJobCreateRequest(
            job_manager.connection, timeout=job.timeout, job_info=job.job_info_serialize())

        # msg's xid is None, give new xid to msg.
        job_manager.connection.set_xid(msg)
        job_manager.add_request(xid=msg.xid, job_obj=job)
        job_manager.connection.send_msg(msg)

        return msg.xid
