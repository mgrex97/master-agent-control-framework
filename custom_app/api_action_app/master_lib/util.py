from ..constant import AGENT_LOCAL, REMOTE_REQUEST_DEFAULT_TIMEOUT, API_ACTION_CONTROLLER_MASTER_APP_NAME as APP_NAME
from async_app_fw.base.app_manager import lookup_service_brick
from custom_app.util.async_api_action import APIAction
from .event import ReqSendRequestFromRemote, ReqRemoteAPILogin

# This decorator make request methods(post,get...) have ability to execute request on agent.
def remote_request_decorator(fun):
    method = getattr(fun, '_method_name')

    async def _remote_send_request(self, *args, agent=None, timeout=REMOTE_REQUEST_DEFAULT_TIMEOUT,method_name=method, **kwargs):
        agent = agent or self.default_agent

        if agent == AGENT_LOCAL:
            res = await fun(self, *args, **kwargs)
        else:
            # send event to APIActionMasterController to deal with remote request.
            res = await ReqSendRequestFromRemote.send_request(agent, method_name, self, *args, timeout=timeout, **kwargs)

            if (callback := (kwargs.get('callback', None))) is not None:
                callback(res)
 
            return res
        
        return res

    return _remote_send_request

# decorate init method
def api_action_init_decorator(cls: APIAction):
    init_method = cls.__init__

    # When APIAction init register APIAction to APIActionMasterController and set default_agent.
    def __init__(self: APIAction, *args, default_agent=AGENT_LOCAL, **kwargs):
        init_method(self, *args, **kwargs)
        self.default_agent = default_agent
        api_action_controller = lookup_service_brick(APP_NAME)
        api_action_controller.register_api_action(self)
 
    def set_default_agent(self, default_agent):
        self.default_agent = default_agent
 
    cls.__init__ = __init__
    cls.set_default_agent = set_default_agent

# decorate login method
def remote_login_decorator(cls: APIAction):
    login_method = cls.login_api

    async def login_api(self, *args, agent=None, **kwargs):
        agent = agent or self.default_agent

        if agent == AGENT_LOCAL:
            res = await login_method(self, *args, **kwargs)
        else:
            # Send event to APIActionMasterController to deal with remote login.
            res = await ReqRemoteAPILogin.send_request(self, agent=agent, *args , **kwargs)

            if (callback := (kwargs.get('callback', None))) is not None:
                callback(res)
        
        return res
 
    cls.login_api = login_api

def add_remote_feature_to_APIAction(cls: APIAction, request_method_names=('get', 'post', 'put', 'delete', 'patch')):
    if type(cls) != type(APIAction):
        raise TypeError("Input variable cls should be class APIAction.")
 
    if getattr(cls, '_remote_feature', None) is not None:
        raise Exception('APIAction already has remote feature.')

    for name in request_method_names:
        new_method = remote_request_decorator(getattr(APIAction, name))
        setattr(APIAction, name, new_method)

    api_action_init_decorator(APIAction)
    remote_login_decorator(APIAction)

    setattr(cls, '_remote_feature', True)