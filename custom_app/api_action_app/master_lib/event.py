from ..constant import API_ACTION_CONTROLLER_MASTER_APP_NAME as APP_NAME, AGENT_LOCAL\
    , DEFAULT_LOGIN_TIMEOUT
from custom_app.util.async_api_action import APIAction, SessionInfo
from async_app_fw.event.async_event import EventAsyncRequestBase

class ReqRemoteAPILogin(EventAsyncRequestBase):
    REQUEST_NAME = 'API login'
    DST_NAME = APP_NAME

    def __init__(self, api_action, *args, agent=AGENT_LOCAL, timeout=DEFAULT_LOGIN_TIMEOUT, **kwargs):
        super().__init__(timeout=timeout)

        if not isinstance(api_action, APIAction):
            self.push_reply(TypeError('Input Variable is not instance of APIAction (async ver).'))
            return

        self.api_action = api_action
        self.timeout = timeout
        self.session_info = api_action.get_session_info()
        self.agent = agent
        self.args = args
        self.kwargs = kwargs

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