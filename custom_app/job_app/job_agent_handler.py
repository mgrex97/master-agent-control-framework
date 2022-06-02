import logging
from custom_app.job_app.job_class import JOB_ASYNC, Job
from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.event import event
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event
from eventlet_framework.controller.mcp_controller.mcp_state import MC_STABLE
from custom_app.job_app.job_manager import JobManager

_REQUIRED_APP = [
    'eventlet_framework.controller.mcp_controller.agent_handler']

LOG = logging.getLogger('custom_app.job_app.jog_agent_handler')


class JobAgentHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'job_agent_handler'
        self.job_manager: JobManager = None
        self.connection = None

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def connect_to_master(self, ev):
        if self.job_manager is not None:
            del self.job_manager

        self.job_manager = JobManager(ev.connection)
        self.connection = ev.connection

    @observe_event(mcp_event.EventMCPJobCreateRequest, MC_STABLE)
    def job_create_request_handler(self, ev):
        LOG.info(f"Get job create request from {self.connection.address}")
        conn = self.connection
        xid = ev.msg.xid

        new_job_id = self.job_manager.get_new_job_id()
        job_obj = Job.create_job_by_job_info(
            self.connection, ev.msg.job_info, new_job_id)

        self.job_manager.add_job(job_obj)

        LOG.info(f"Job Info:{ev.msg.job_info}")

        msg = conn.mcproto_parser.MCPJobCreateReply(
            conn, new_job_id, job_info=job_obj.job_info_serialize())
        msg.xid = xid
        conn.send_msg(msg)

    @observe_event(mcp_event.EventMCPJobACK, MC_STABLE)
    def job_ack_hanlder(self, ev):
        job: Job = self.job_manager.get_job(ev.msg.job_id)
        job.change_state(JOB_ASYNC)
        job.run_job()
