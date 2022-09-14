from socket import timeout
from async_app_fw.event.event import EventBase
from async_app_fw.event.async_event import EventAsyncRequestBase
from custom_app.util.async_command_executor import AsyncCommandExecutor
from ..constant import COMMAND_EXECUTOR_MASTER_HANDLER_APP_NAME as APP_NAME

class EventRemoteExecute(EventBase):
    def __init__(self, cmd_exe: AsyncCommandExecutor, args, kwargs):
        super(EventRemoteExecute, self).__init__()
        self.cmd_exe = cmd_exe
        self.input_vars = {
            'args': args,
            'kwargs': kwargs
        }

class EventRemoteCancelExecute(EventBase):
    def __init__(self, cmd_exe):
        super(EventRemoteCancelExecute, self).__init__()
        self.cmd_exe = cmd_exe

class ReqReadStd(EventAsyncRequestBase):
    DST_NAME = APP_NAME
    REQUEST_NAME = 'Remote read stdout'

    def __init__(self, cmd_exe, std_type, args, kwargs, timeout=None):
        super().__init__(timeout)
        self.cmd_exe = cmd_exe
        self.std_type = std_type
        self.input_vars = {
            'args': args,
            'kwargs': kwargs
        }

class ReqWriterStd(EventAsyncRequestBase):
    DST_NAME = APP_NAME
    REQUEST_NAME = 'Remote writer stdin'

    def __init__(self, cmd_exe, *args, **kwargs):
        timeout = kwargs.get('timeout', None)
        super().__init__(timeout)
        self.cmd_exe = cmd_exe
        self.input_vars = {
            'args': args,
            'kwargs': kwargs
        }