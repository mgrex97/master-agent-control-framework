from custom_app.job_app.job_class import JOB_ASYNC, Job
from eventlet_framework.protocol.mcp.mcp_parser_v_1_0 import MCPJobCreateReply
from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.event import event
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event, observe_event_without_event_source
from eventlet_framework.controller.mcp_controller.mcp_state import MC_HANDSHAK
from custom_app.job_app.job_manager import JobManager


class JobAgentHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'job_agent_handler'
        self.job_manager: JobManager = None

    @observe_event(event.EventSocketConnecting, MC_HANDSHAK)
    def connect_to_master(self, ev):
        if self.job_manager is not None:
            del self.job_manager
        self.job_manager = JobManager(ev.connection)

    @observe_event(mcp_event.EventMCPJobCreateRequest, MC_HANDSHAK)
    def job_create_request_handler(self, ev):
        conn = ev.connection
        xid = ev.msg.xid

        new_job_id = self.job_manager.get_new_job_id()
        job_obj = Job.create_job_by_job_info(ev.msg.job_info, new_job_id)

        self.job_manager.add_job(job_obj)

        msg = conn.mcproto_parser.MCPJobCreateReply(
            conn, new_job_id, job_info=job_obj.job_info_serialize())
        msg.set_xid(xid)
        conn.send_msg(msg)

    @observe_event(mcp_event.EventMCPJobACK, MC_HANDSHAK)
    def master_job_ack_handler(self, ev):
        job_obj: Job = self.job_manager.get_job(ev.msg.job_id)
        job_obj.change_state(JOB_ASYNC)
        job_obj.run_job()
