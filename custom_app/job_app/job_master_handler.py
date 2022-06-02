import logging
from custom_app.job_app.job_class import JOB_ASYNC, JobCommand
from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.event import event
from eventlet_framework.event.mcp_event import mcp_event
from eventlet_framework.controller.handler import observe_event
from eventlet_framework.controller.mcp_controller.mcp_state import MC_STABLE
from custom_app.job_app.job_manager import JobManager

_REQUIRED_APP = ['eventlet_framework.controller.mcp_controller.master_handler']

LOG = logging.getLogger('custom_app.job_app.jog_master_handler')


class JobMasterHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = 'job_master_handler'
        self.job_managers = {}

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def agent_join(self, ev):
        conn = ev.connection
        self.job_managers[conn.id] = JobManager(
            ev.connection)

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
        pass

    def job_delete(self, connection_id, job_id):
        self.job_managers[connection_id].get_job(job_id)

    @observe_event(mcp_event.EventMCPJobOutput, MC_STABLE)
    def job_output_handler(self, ev):
        conn_id = ev.msg.connection.id
        job_id = ev.msg.job_id
        job = self.job_managers[conn_id].get_job(job_id)
        job.output_handler(ev.msg.job_info)

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
