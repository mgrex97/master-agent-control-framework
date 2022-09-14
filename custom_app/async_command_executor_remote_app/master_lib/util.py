import asyncio
from types import MethodType
from async_app_fw.controller.mcp_controller.master_lib.event import ReqGetAgentConnection
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from ..constant import STDERR_ID, STDOUT_ID
from custom_app.util.async_command_executor import AsyncCommandExecutor
from custom_app.util.constant import AsyncUtilityEventID
from .event import EventRemoteExecute, EventRemoteCancelExecute, ReqReadStd


spawn = app_hub.spawn

async def remote_execute(app: BaseApp, cmd_service: AsyncCommandExecutor, args, kwargs):
    app.send_event_to_self(EventRemoteExecute(cmd_service, args, kwargs))
    await cmd_service._wait_event(AsyncUtilityEventID.stop.value, timeout=None)

    # clean _mcp_connection.
    cmd_service._mcp_connection = None

def remote_spawn_execute(method, app: BaseApp):
    def _spawn_execute_remote_compatible(self: AsyncCommandExecutor, *args, **kwargs):
        if getattr(self, '_mcp_connection', None) is not None:
            self._execute_task = spawn(remote_execute, app, self, args, kwargs)
        else:
            # method function, no need to pass self.
            method(*args, **kwargs)
 
    return _spawn_execute_remote_compatible

def remote_cancel_execute(method, app: BaseApp):
    def _cancel_execute_remote_compatible(self: AsyncCommandExecutor):
        if getattr(self, '_mcp_connection', None) is not None:
            event = EventRemoteCancelExecute(self)
            app.send_event_to_self(event)
        else:
            method()

    return _cancel_execute_remote_compatible 

def remote_start(method):
    async def _start_with_remote_compatible(self: AsyncCommandExecutor, *args, agent=None, **kwargs):
        if agent is not None:
            conn = await ReqGetAgentConnection.send_request(agent, timeout=5)
            self._mcp_connection = conn
 
        await method(*args, **kwargs)

    return _start_with_remote_compatible

def remote_read_std(method, std_type):
    async def _read_std_with_remote_comaptible(self: AsyncCommandExecutor, *args, **kwargs):
        if getattr(self, '_mcp_connection', None) is not None:
            return await ReqReadStd.send_request(self, std_type, args, kwargs)
        else:
            return await method(*args, **kwargs)

    return _read_std_with_remote_comaptible

def remote_feature_newer(app: BaseApp):
    def add_remote_feature(cls: AsyncCommandExecutor, *args, **kwargs):
        instance:AsyncCommandExecutor= object().__new__(cls)

        app.regiseter_cmd_service(instance)
        instance._mcp_connection = None
        instance._spwan_execute = MethodType(remote_spawn_execute(instance._spwan_execute, app), instance)
        instance._cancel_execute = MethodType(remote_cancel_execute(instance._cancel_execute, app), instance)
        instance.read_stdout = MethodType(remote_read_std(instance.read_stdout, STDOUT_ID), instance)
        instance.read_stderr = MethodType(remote_read_std(instance.read_stderr, STDERR_ID), instance)
        instance.start = MethodType(remote_start(instance.start), instance)

        return instance
 
    return add_remote_feature