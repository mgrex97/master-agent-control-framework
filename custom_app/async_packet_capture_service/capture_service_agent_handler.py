from asyncio import CancelledError
import asyncio
from typing import Dict
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.agent_controller import AgentConnection
from async_app_fw.controller.mcp_controller.mcp_state import MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from custom_app.async_packet_capture_service.agent_lib.util import add_remote_feature
from custom_app.util.async_tshark import AsyncCaptureService
from .constant import CAPTURE_SERVCIE_AGENT_HANDLER_APP_NAME as APP_NAME, CAPTURE_SERVICE_CLS_ID_MAPPING
from async_app_fw.lib.hub import app_hub

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.agent_handler']

spawn = app_hub.spawn

class CaptureServiceAgentHandler(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)

        self.name = APP_NAME
        self.master_connection:AgentConnection = None
        self.service: Dict[int, AsyncCaptureService] = {}
        self.service_tasks: Dict[int, asyncio.Task] = {}
        AsyncCaptureService.__new__ = add_remote_feature

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def connection_handler(self, ev):
        self.master_connection = ev.connection

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def disconnection_handler(self, ev):
        # cleanup service and service_tasks that was saved before.
        for service_task in self.service_tasks.values():
            service_task.cancel()

        self.service.clear()
        self.service_tasks.clear()

    async def stop(self):
        for task in self.service_tasks.values():
            task.cancel()
 
        await asyncio.gather(*self.service_tasks.values())

        return await super().stop()

    async def run_capture_service(self, capture: AsyncCaptureService, input_vars):
        try:
            conn = self.master_connection
            capture_id = capture._capture_id
    
            async def send_packet_to_master(pkt):
                msg = conn.mcproto_parser.CaptureServiceSendPKT(conn, capture_id, pkt)
                conn.send_msg(msg)
 
            input_vars['kwargs']['callback'] = send_packet_to_master
 
            conn = self.master_connection
            capture._reset()
            capture._spwan_execute(*input_vars['args'], **input_vars['kwargs'])
            # wait until service finish.
            await capture.wait_finished(timeout=None)
        except CancelledError:
            await capture.stop()
        finally:
            self.service_tasks.pop(capture_id)

    @observe_event(mcp_event.EventCaptureServiceCancelExecute)
    def stop_capture_service(self, ev):
        msg = ev.msg
        capture_id = msg.capture_id

        if (capture := self.service.get(capture_id, None)) is not None:
            # service not found.
            return

        spawn(capture.stop)
 
    @observe_event(mcp_event.EventCaptureServiceExe)
    def execute_handler(self, ev):
        msg = ev.msg
        capture_id = msg.capture_id

        # Capture Service has not instantiated.
        if (capture_instance := self.service.get(capture_id, None)) is None:
            capture_service_cls = CAPTURE_SERVICE_CLS_ID_MAPPING[msg.capture_service_cls_id]
            capture_instance:AsyncCaptureService = capture_service_cls()
            capture_instance.set_connection(self.master_connection)
            capture_instance._capture_id = capture_id
            # save capture serivce's instance
            self.service[capture_id] = capture_instance

        # spawn a task to run capture service
        service_task = spawn(self.run_capture_service, capture_instance, msg.input_vars)
        # save task
        self.service_tasks[capture_id] = service_task
