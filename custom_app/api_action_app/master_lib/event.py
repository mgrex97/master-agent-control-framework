from ..constant import API_ACTION_CONTROLLER_MASTER_APP_NAME as APP_NAME, AGENT_LOCAL\
    , DEFAULT_SESSION_INIT_TIMEOUT, DEFAULT_LOGIN_TIMEOUT
from custom_app.util.async_api_action import SessionInfo
from async_app_fw.event.async_event import EventAsyncRequestBase

class ReqSessionInit(EventAsyncRequestBase):
    REQUEST_NAME = 'API Session Init'
    DST_NAME = APP_NAME

    def __init__(self, api_hostname, base_url, login_info=None, auth=None, timeout=DEFAULT_SESSION_INIT_TIMEOUT):
        super().__init__(timeout)
        self.session_info = SessionInfo(
            api_hostname,
            base_url,
            login_info,
            auth
        )

class ReqAPILoginCheck(EventAsyncRequestBase):
    REQUEST_NAME = 'API login check'
    DST_NAME = APP_NAME

    def __init__(self, api_hostname, session_info=None, agent_address=AGENT_LOCAL, timeout=DEFAULT_LOGIN_TIMEOUT):
        super().__init__(timeout=timeout)
        self.api_hostname = api_hostname
        self.session_info = session_info
        self.agent_address = agent_address

class ReqSendRequestFromRemote(EventAsyncRequestBase):
    REQUEST_NAME = 'Rquest from remote agent.'
    DST_NAME = APP_NAME

    def __init__(self, agent, method, api_action, *args, timeout=None, **kwargs):
        super().__init__(timeout=timeout)
        self.agent = agent
        self.method = method
        self.api_action = api_action
        self.args = args
        self.kwargs = kwargs

class ReqGetAPIAction(EventAsyncRequestBase):
    REQUEST_NAME = 'Get API Action'
    DST_NAME = APP_NAME

    def __init__(self, api_hostname, timeout=None):
        super().__init__(timeout=timeout)
        self.api_hostname = api_hostname
