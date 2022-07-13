import asyncio
import logging
from custom_app.job_app.job_util.job_class import REMOTE_AGENT, Job
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.event import event
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_STABLE
from async_app_fw.protocol.mcp.mcp_parser_v_1_0 import MCPJobFeatureExe, MCPJobStateChange
from custom_app.job_app.job_manager import JobManager

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.agent_handler']

APP_NAME_JobAgentHandler = 'job_agent_handler'


class JobAgentHandler(BaseApp):
    LOG = logging.getLogger('Job Agent Handler')

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME_JobAgentHandler
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
            self.connection, ev.msg.job_info, new_job_id, REMOTE_AGENT)

        self.job_manager.add_job(job_obj)

        job_info = job_obj.job_info_serialize()
        self.LOG.info(f"Job Create Info:{job_info}")

        msg = conn.mcproto_parser.MCPJobCreateReply(
            conn, new_job_id, job_info=job_info)
        msg.xid = xid
        conn.send_msg(msg)

    @observe_event(mcp_event.EventMCPJobStateChange, MC_STABLE)
    def job_state_change_hanlder(self, ev):
        msg: MCPJobStateChange = ev.msg
        job: Job = self.job_manager.get_job(ev.msg.job_id)
        job.remote_change_state(msg.before, msg.after, msg.info)

    @observe_event(mcp_event.EventMCPJobDeleteRequest, MC_STABLE)
    def job_delete_request_hanlder(self, ev):
        self.LOG.info(f"Delete Job request, Job ID <{ev.msg.job_id}>")
        self.job_manager.del_job(ev.msg.job_id)

    @observe_event(mcp_event.EventMCPJobDeleteAll, MC_STABLE)
    def job_delete_all_handler(self, ev):
        self.LOG.info(f"Delete All Jobs.")
        self.job_manager.delete_all_job()

    @observe_event(mcp_event.EventMCPJobFeatureExe, MC_STABLE)
    def job_feature_exe_handler(self, ev):
        msg: MCPJobFeatureExe = ev.msg
        job: Job = self.job_manager.get_job(msg.job_id)
        job.exe_feature(msg.info)
