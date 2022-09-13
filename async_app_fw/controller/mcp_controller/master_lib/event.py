from async_app_fw.event.async_event import EventAsyncRequestBase
from .constant import APP_NAME

class ReqGetAgentConnection(EventAsyncRequestBase):
    REQUEST_NAME = 'Requet, Check Agent Exist.'
    DST_NAME = APP_NAME

    def __init__(self, agent, timeout=5):
        super().__init__(timeout)
        self.agent = agent