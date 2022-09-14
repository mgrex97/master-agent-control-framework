import logging
from types import MethodType
from ..constant import STDERR_ID, STDOUT_ID
from custom_app.util.async_command_executor import AsyncCommandExecutor


def remote_set_event(method):
    def _remote_set_event(self, event_id):
        connection = self._connection
        method(event_id)
        msg = connection.mcproto_parser.CmdServiceSetEvent(connection, self._cmd_id, event_id)
        connection.send_msg(msg)

    return _remote_set_event

def remote_set_exception(method):
    def _remote_set_exception(self, exception):
        connection = self._connection
        method(exception)
        msg = connection.mcproto_parser.CmdServiceSetException(connection, self._cmd_id, exception)
        connection.send_msg(msg)

    return _remote_set_exception

async def read_std(self: AsyncCommandExecutor, std_type=None, *args, **kwargs):
    if std_type == STDOUT_ID:
        return await self.read_stdout(*args, **kwargs)
    elif std_type == STDERR_ID:
        return await self.read_stderr(*args, **kwargs)
    else:
        logging.warning(f"Unknow std_type {std_type}.")

def add_remote_feature(cls: AsyncCommandExecutor, *args, **kwargs):
    instance:AsyncCommandExecutor = object().__new__(cls)

    instance._set_event = \
        MethodType(remote_set_event(instance._set_event), instance)
    instance._set_exception = \
        MethodType(remote_set_exception(instance._set_exception), instance)

    def set_connection(self, connection):
        self._connection = connection

    instance.read_std = MethodType(read_std, instance)
    instance.set_connection = MethodType(set_connection, instance)

    return instance