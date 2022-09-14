from async_app_fw.event.event import EventBase
from custom_app.util.async_tshark import AsyncCaptureService

class EventRemoteExecute(EventBase):
    def __init__(self, capture: AsyncCaptureService, args, kwargs):
        super(EventRemoteExecute, self).__init__()
        self.capture = capture
        self.input_vars = {
            'args': args,
            'kwargs': kwargs
        }

class EventRemoteCancelExecute(EventBase):
    def __init__(self, capture):
        super(EventRemoteCancelExecute, self).__init__()
        self.capture = capture
