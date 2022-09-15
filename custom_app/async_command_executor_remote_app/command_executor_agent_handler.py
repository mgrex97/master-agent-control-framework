from asyncio import CancelledError
from typing import Dict
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from custom_app.async_command_executor_remote_app.agent_lib.util import add_remote_feature
from .constant import COMMAND_EXECUTOR_AGENT_HANDLER_APP_NAME as APP_NAME
from custom_app.util.async_command_executor import AsyncCommandExecutor

spawn = app_hub.spawn

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.agent_handler']

class CommandServiceNotExisted(Exception):
    pass

class CommandExecutorAgentHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)

        self.name = APP_NAME
        self.master_connection = None
        self.executors: Dict[int, AsyncCommandExecutor] = {}
        self.executors_tasks = {}
        AsyncCommandExecutor.__new__ = add_remote_feature

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def connection_handler(self, ev):
        self.master_connection = ev.connection

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def connection_handler(self, ev):
        self.master_connection = ev.connection

    async def read_std(self, executor:AsyncCommandExecutor, std_type, args, kwargs, xid):
        connection = self.master_connection
        try:
            output = await executor.read_std(*args, std_type=std_type, **kwargs)
            msg = connection.mcproto_parser.CmdServiceReadStdRes(connection, executor._cmd_id, output)
            msg.xid = xid
            connection.send_msg(msg)
        except Exception as e:
            if executor._cmd_id in self.executors:
                msg = connection.mcproto_parser.CmdServiceReadStdException(connection, executor._cmd_id, e)
                msg.xid = xid
                connection.send_msg(msg)
                return


    @observe_event(mcp_event.EventCmdServiceReadStd)
    def read_stdout_handler(self, ev):
        msg = ev.msg

        if (executor := self.executors.get(msg.cmd_id, None)) is None:
            exception = CommandServiceNotExisted(f'Service ID {msg.cmd_id}')
            conn = self.master_connection
            msg = conn.mcproto_parser.CmdServiceReadStdException(conn, msg.cmd_id, exception)
            return

        input_vars = msg.input_vars
        spawn(self.read_std, executor, msg.std_type, input_vars['args'], input_vars['kwargs'], msg.xid)

    async def write_std(self, cmd_exe: AsyncCommandExecutor, args, kwargs):
        cmd_exe.write_stdin()

    # @observe_event(mcp_event.EventCmdServiceWriterStd)
    def write_stdin_handler(self, ev):
        pass

    @observe_event(mcp_event.EventCmdServiceCancelExecute)
    def stop_cmd_service(self, ev):
        msg = ev.msg
        cmd_id = msg.cmd_id

        if (cmd_exe := self.executors.get(cmd_id, None)) is None:
            # service not found.
            return

        spawn(cmd_exe.stop)

    async def run_command(self, cmd_exe: AsyncCommandExecutor, input_vars):
        try:
            cmd_exe._reset()
            cmd_exe._spwan_execute(*input_vars['args'], **input_vars['kwargs'])
            await cmd_exe.wait_finished(timeout=None)
        except CancelledError:
            await cmd_exe.stop()
        finally:
            self.executors_tasks.pop(cmd_exe._cmd_id)

    @observe_event(mcp_event.EventCmdServiceExecute)
    def execute_handler(self, ev):
        msg = ev.msg
        cmd_id = msg.cmd_id

        # Capture Service has not instantiated.
        if (cmd_exe_instance := self.executors.get(cmd_id, None)) is None:
            cmd_exe_instance = AsyncCommandExecutor()
            cmd_exe_instance.set_connection(self.master_connection)
            cmd_exe_instance._cmd_id = cmd_id
            # save capture serivce's instance
            self.executors[cmd_id] = cmd_exe_instance

        # spawn a task to run capture service
        service_task = spawn(self.run_command, cmd_exe_instance, msg.input_vars)
        # save task
        self.executors_tasks[cmd_id] = service_task