import asyncio
import logging
from custom_app.job_app.async_job_app.job_class import JOB_ASYNC, Job
from eventlet_framework.base.async_app_manager import BaseApp
from eventlet_framework.event import event
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event
from eventlet_framework.controller.mcp_controller.async_ver.mcp_state import MC_DISCONNECT, MC_STABLE
from custom_app.job_app.async_job_app.job_manager import JobManager
from eventlet_framework.lib.async_hub import TaskLoop, app_hub

_REQUIRED_APP = [
    'eventlet_framework.controller.mcp_controller.async_ver.agent_handler']


class JobAgentHandler(BaseApp):
    LOG = logging.getLogger('Job Agent Handler')

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'job_agent_handler'
        self.job_manager: JobManager = JobManager()
        self.connection = None
        self.job_clear_task_loop = None

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def master_connection_stable(self, ev):
        self.job_manager.connection = ev.connection
        self.connection = ev.connection

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def disconnecting_handler(self, ev: event.EventSocketConnecting):
        self.LOG.info('Master disconnect, Clear Job...')
        self.job_manager.connection = None
        self.job_manager.delete_all_job()

    @observe_event(mcp_event.EventMCPJobCreateRequest, MC_STABLE)
    def job_create_request_handler(self, ev):
        self.LOG.info(f"Get job create request from {self.connection.address}")
        conn = self.connection
        xid = ev.msg.xid

        new_job_id = self.job_manager.get_new_job_id()
        job_obj = Job.create_job_by_job_info(
            self.connection, ev.msg.job_info, new_job_id)

        self.job_manager.add_job(job_obj)

        job_info = job_obj.job_info_serialize()
        self.LOG.info(f"Job Create Info:{job_info}")

        msg = conn.mcproto_parser.MCPJobCreateReply(
            conn, new_job_id, job_info=job_info)
        msg.xid = xid
        conn.send_msg(msg)

    @observe_event(mcp_event.EventMCPJobACK, MC_STABLE)
    def job_ack_hanlder(self, ev):
        job: Job = self.job_manager.get_job(ev.msg.job_id)
        job.change_state(JOB_ASYNC)
        job.run_job()

    @observe_event(mcp_event.EventMCPJobDeleteRequest, MC_STABLE)
    def job_delete_request_hanlder(self, ev):
        self.LOG.info(f"Delete Job request, Job ID <{ev.msg.job_id}>")
        self.job_manager.del_job(ev.msg.job_id)
        # conn = ev.connection
        # reply delete result
        # reply_msg = ev.msg.cls_msg_type(ev.connection, ev.msg.job_id)
        # conn.set_xid(reply_msg)

    @observe_event(mcp_event.EventMCPJobDeleteAll, MC_STABLE)
    def job_delete_all_handler(self, ev):
        self.LOG.info(f"Delete All Jobs.")
        self.job_manager.delete_all_job()
