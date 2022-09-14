import traceback
from typing import Dict
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.event.async_event import EventAsyncRequestBase
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.master_controller import MasterConnection
from custom_app.util.async_command_executor import AsyncCommandExecutor
from custom_app.util.async_tshark import AsyncCaptureService
from .constant import COMMAND_EXECUTOR_MASTER_HANDLER_APP_NAME as APP_NAME
from .master_lib.event import EventRemoteExecute, EventRemoteCancelExecute, ReqReadStd, ReqWriterStd
from .master_lib.util import remote_feature_newer

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']

class CommandExecutorMasterHandler(BaseApp):
    CMD_EXECUTOR_ID = 0

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)

        self.name = APP_NAME
        self.agent_connection: Dict[str, MasterConnection] = {} 
        self.cmd_service: Dict[int, AsyncCaptureService] = {}
        self.req_tmp_pool: Dict[int, EventAsyncRequestBase] = {}

    def start(self):
        tasks = super().start()

        if not getattr(AsyncCommandExecutor, '_remote_feautre_apply', None):
            AsyncCommandExecutor.__new__ = remote_feature_newer(self)
            setattr(AsyncCaptureService, '_remote_feature_apply', True)
        
        return tasks

    async def close(self):
        return await super().close()

    @classmethod
    def get_new_cmd_executor_id(cls):
        id = cls.CMD_EXECUTOR_ID
        cls.CMD_EXECUTOR_ID = id + 1
        return id

    def regiseter_cmd_service(self, cmd_executor):
        cmd_id = self.get_new_cmd_executor_id()
        cmd_executor._cmd_id = cmd_id
        self.cmd_service[cmd_id] = cmd_executor

    @observe_event(EventRemoteExecute)
    def remote_execute_handler(self, ev: EventRemoteExecute):
        # check capture service have registed or not.
        cmd_exe:AsyncCaptureService = ev.cmd_exe
        conn:MasterConnection = cmd_exe._mcp_connection

        # check instance connection
        """
        try:
            # check_connection(conn)
        except Exception as e:
            capture._set_exception(e)
            capture._cancel_execute()
            return
        """

        # send input variables that execute method need to remote agnet.
        msg = conn.mcproto_parser.CmdServiceExecute(conn, cmd_exe._cmd_id, ev.input_vars)
        conn.send_msg(msg)

    @observe_event(EventRemoteCancelExecute)
    def remote_cancel_execute(self, ev: EventRemoteCancelExecute):
        cmd_exe:AsyncCommandExecutor = ev.cmd_exe
        conn:MasterConnection = cmd_exe._mcp_connection

        # check instance connection
        """
        try:
            check_connection(conn)
        except Exception as e:
            capture._set_exception(e)
            capture._cancel_execute()
            return
        """

        msg = conn.mcproto_parser.CmdServiceCancelExecute(conn, cmd_exe._cmd_id)
        conn.send_msg(msg)

    @observe_event(ReqReadStd)
    def remote_read_stdout(self, ev):
        cmd_exe = ev.cmd_exe
        conn:MasterConnection = cmd_exe._mcp_connection
        msg = conn.mcproto_parser.CmdServiceReadStd(conn, cmd_exe._cmd_id, ev.std_type, ev.input_vars)
        try:
            msg.serialize()
        except Exception:
            print(traceback.format_exc())
        xid = conn.set_xid(msg)
        conn.send_msg(msg)
        # register Async Event Request
        self.req_tmp_pool[xid] = ev

    @observe_event(ReqWriterStd)
    def remote_writer_stdin(self, ev):
        cmd_exe = ev.cmd_exe
        conn:MasterConnection = cmd_exe._mcp_connection
        msg = conn.mcproto_parser.CmdServiceWriteStd(conn, cmd_exe._cmd_id, ev.inputs_vars)
        xid = conn.set_xid(msg)
        conn.send_msg(msg)
        # register Async Event Request
        self.req_tmp_pool[xid] = ev

    @observe_event(mcp_event.EventCmdServiceWriteStdException)
    @observe_event(mcp_event.EventCmdServiceReadStdException)
    def read_stdout_result_handler(self, ev):
        if (req := self.req_tmp_pool.get(ev.msg.xid, None)) is None:
            return

        req.push_reply(ev.exception)

    @observe_event(mcp_event.EventCmdServiceReadStdRes)
    @observe_event(mcp_event.EventCmdServiceWriteStdRes)
    def read_stdout_result_handler(self, ev):
        msg = ev.msg
        if (req := self.req_tmp_pool.get(msg.xid, None)) is None:
            return

        req.push_reply(msg.output)

    @observe_event(mcp_event.EventCmdServiceSetException)
    def set_exception_by_remote(self, ev):
        msg = ev.msg
        self.cmd_service[msg.cmd_id]._set_exception(msg.exception)

    @observe_event(mcp_event.EventCmdServiceSetEvent)
    def event_set_by_remote_agent(self, ev):
        msg = ev.msg
        self.cmd_service[msg.cmd_id]._set_event(msg.event_id)