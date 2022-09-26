from typing import Dict
import asyncio

from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.master_controller import MasterConnection
from async_app_fw.controller.mcp_controller.mcp_state import MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.lib.hub import app_hub
from custom_app.util.async_tshark import AsyncCaptureService

from .master_lib.event import EventRemoteExecute, EventRemoteCancelExecute
from .master_lib.exception import ConnectionIsNoneWhenRemoteExecute, ConnectionIsNotStable
from .master_lib.util import remote_feature_newer
from .constant import CAPTURE_SERVCIE_MASTER_HANDLER_APP_NAME as APP_NAME, SERVICE_CLOSE_MONITER_INTERVAL

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']

spawn = app_hub.spawn

class CaptureServiceMasterHandler(BaseApp):
    CaptureService_ID = 0

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)

        self.name = APP_NAME
        self.agent_connection: Dict[str, MasterConnection] = {} 
        self.capture_services: Dict[int, AsyncCaptureService] = {}

    def start(self):
        task = super().start()
 
        if not getattr(AsyncCaptureService, '_remote_feature_apply', None):
            AsyncCaptureService.__new__ = remote_feature_newer(self)
            setattr(AsyncCaptureService, '_remote_feature_apply', True)

        return task

    async def close(self):
        pass

    @classmethod
    def get_new_capture_id(cls):
        id = cls.CaptureService_ID
        cls.CaptureService_ID = id + 1
        return id

    def register_capture(self, capture_instance):
        capture_id = self.get_new_capture_id()
        capture_instance._capture_id = capture_id
        self.capture_services[capture_id] = capture_instance
    
    @staticmethod
    def check_connection(connection):
        if connection is None:
            raise ConnectionIsNoneWhenRemoteExecute()
        elif not isinstance(connection, MasterConnection):
            raise TypeError(f"Connection should be instance of {MasterConnection.__name__}.")
        elif connection.state != MC_STABLE:
            raise ConnectionIsNotStable()

    @observe_event(EventRemoteExecute)
    def remote_execute_handler(self, ev: EventRemoteExecute):
        # check capture service have registed or not.
        capture:AsyncCaptureService = ev.capture
        conn:MasterConnection = capture._mcp_connection

        # check instance connection
        try:
            CaptureServiceMasterHandler.check_connection(conn)
        except Exception as e:
            capture._set_exception(e)
            capture._cancel_execute()
            return

        # send input variables that execute method need to remote agnet.
        msg = conn.mcproto_parser.CaptureServiceExe(conn, capture._capture_id, capture.capture_cls_id, ev.input_vars)
        conn.send_msg(msg)

    @observe_event(EventRemoteCancelExecute)
    def remote_cancel_execute(self, ev):
        capture:AsyncCaptureService = ev.capture
        conn = capture._mcp_connection

        # check instance connection
        try:
            CaptureServiceMasterHandler.check_connection(conn)
        except Exception as e:
            capture._set_exception(e)
            capture._cancel_execute()
            return

        msg = conn.mcproto_parser.CaptureServiceCancelExecute(conn, capture._capture_id)
        conn.send_msg(msg)

    @observe_event(mcp_event.EventCaptureServiceSendPKT)
    def receive_packet_from_remote(self, ev):
        msg = ev.msg
        spawn(self.capture_services[msg.capture_id]._capture.push_packet, msg.pkt)

    @observe_event(mcp_event.EventCaptureServiceSetException)
    def set_exception_by_remote(self, ev):
        msg = ev.msg
        self.capture_services[msg.capture_id]._set_exception(msg.exception)

    @observe_event(mcp_event.EventCaptureServiceSetEvent)
    def event_set_by_remote_agent(self, ev):
        msg = ev.msg
        self.capture_services[msg.capture_id]._set_event(msg.event_id)