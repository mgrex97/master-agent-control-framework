from multiprocessing import connection
from types import MethodType
from custom_app.util.async_tshark import AsyncCaptureService
from async_app_fw.controller.mcp_controller.agent_controller import AgentConnection

def remote_set_event(method):
    def _remote_set_event(self, event_id):
        connection = self._connection
        method(event_id)
        msg = connection.mcproto_parser.CaptureServiceSetEvent(connection, self._capture_id, event_id)
        connection.send_msg(msg)

    return _remote_set_event

def remote_set_exception(method):
    def _remote_set_exception(self, exception):
        connection = self._connection
        method(exception)
        msg = connection.mcproto_parser.CaptureServiceSetException(connection, self._capture_id, exception)
        connection.send_msg(msg)

    return _remote_set_exception

def add_remote_feature(cls: AsyncCaptureService, *args, **kwargs):
    instance:AsyncCaptureService = object().__new__(cls)

    instance._set_event = \
        MethodType(remote_set_event(instance._set_event), instance)
    instance._set_exception = \
        MethodType(remote_set_exception(instance._set_exception), instance)

    def set_connection(self, connection):
        self._connection = connection

    instance.set_connection = MethodType(set_connection, instance)

    return instance