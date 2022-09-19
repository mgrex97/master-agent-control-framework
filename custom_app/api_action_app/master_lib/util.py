from types import MethodType
from ..constant import AGENT_LOCAL, REMOTE_REQUEST_DEFAULT_TIMEOUT, API_ACTION_CONTROLLER_MASTER_APP_NAME as APP_NAME
from async_app_fw.base.app_manager import lookup_service_brick
from custom_app.util.async_api_action import APIAction
from .event import ReqSendRequestFromRemote, ReqRemoteAPILogin

# This decorator make request methods(post,get...) have ability to execute request on agent.
def remote_request_decorator(method):
    method_name = getattr(method, '_method_name')

    async def _remote_send_request(self, *args, agent=None, timeout=REMOTE_REQUEST_DEFAULT_TIMEOUT,method_name=method_name, **kwargs):
        agent = agent or self.default_agent

        if agent == AGENT_LOCAL:
            res = await method(self, *args, **kwargs)
        else:
            # send event to APIActionMasterController to deal with remote request.
            res = await ReqSendRequestFromRemote.send_request(agent, method_name, self, *args, timeout=timeout, **kwargs)

            if (callback := (kwargs.get('callback', None))) is not None:
                callback(res)
 
            return res
        
        return res

    return _remote_send_request

# decorate login method
def remote_login_decorator(instance: APIAction):
    login_method = instance.login_api

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
 
    instance.login_api = MethodType(login_api, instance)
 
request_method_names=('get', 'post', 'put', 'delete', 'patch')

def add_remote_feature_to_APIAction(cls: APIAction, *args, **kwargs):
    instance = object().__new__(cls)

    for name in request_method_names:
        new_method = remote_request_decorator(getattr(instance, name))
        setattr(instance, name, MethodType(new_method, instance))

    remote_login_decorator(instance)

    def set_default_agent(self, default_agent):
        self.default_agent = default_agent

    instance.default_agent = AGENT_LOCAL
    instance.set_default_agent = MethodType(set_default_agent, instance)

    api_action_controller = lookup_service_brick(APP_NAME)
    api_action_controller.register_api_action(instance)

    return instance