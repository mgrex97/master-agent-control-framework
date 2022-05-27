from custom_app.job_app.job_class import JOB_ASYNC, JobCommand, Job
from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.event import event
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event, observe_event_without_event_source
from eventlet_framework.controller.mcp_controller.mcp_state import MC_HANDSHAK
from custom_app.job_app.job_manager import JobManager


class JobMasterHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'job_master_handler'
        self.job_managers = {}

    @observe_event(event.EventSocketConnecting, MC_HANDSHAK)
    def agent_join(self, ev):
        self.job_managers[ev.connection.address] = JobManager(
            ev.connection)

    def agent_machine_leave(self, ev):
        del self.job_managers[ev.connection.address]

    @observe_event(mcp_event.EventMCPJobCreateReply, MC_HANDSHAK)
    def job_create_reply_handler(self, ev):
        msg = ev.msg
        conn = ev.connection
        job_id = ev.msg.job_id
        self.job_managers[conn.address].job_id_async(msg.xid, msg.job_id)

        msg = conn.mcproto_parser.MCPCommandACK(conn, job_id)
        conn.send_msg(msg)

    @observe_event(mcp_event.EventMCPJobStateInform, MC_HANDSHAK)
    def job_state_inform_handler(self, ev):
        print('get inform')

    def exe_cmd_on_agent(self, address, command):
        assert address not in self.job_managers

        job_manager: JobManager = self.job_managers[address]
        job = JobCommand(job_manager.connection, command)

        msg = job_manager.connection.mcproto_parser.MCPCommandExecuteRequest(
            job_manager.connection, timeout=10, job_info=job.job_info_serialize())
        job_manager.connection.send_msg(msg)

        job_manager.add_request(xid=msg.xid, job_obj=job)
