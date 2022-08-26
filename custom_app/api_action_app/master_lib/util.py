from ..constant import AGENT_LOCAL, REMOTE_REQUEST_DEFAULT_TIMEOUT
from custom_app.util.async_api_action import APIAction
from .event import ReqSendRequestFromRemote

# This decorator make request methods(post,get...) have ability to execute request on agent.
def remote_request_decorator(fun):
    method = getattr(fun, '_method_name')

    async def _remote_send_request(self, *args, agent=None, timeout=REMOTE_REQUEST_DEFAULT_TIMEOUT,method_name=method, **kwargs):
        if getattr(self, 'default_agent', None) is None:
            self.default_agent = AGENT_LOCAL

        agent = agent or self.default_agent

        if agent == AGENT_LOCAL:
            res = await fun(self, *args, *kwargs)
        else:
            # send request (Event) to APIActionMasterController to deal with remote request.
            res = await ReqSendRequestFromRemote.send_request(agent, method_name, self, *args, timeout=timeout, **kwargs)

            if (callback := (kwargs.get('callback', None))) is not None:
                callback(res)
 
            return res
        
        return res

    return _remote_send_request

def remote_request_init_decorator(cls: APIAction):
    init_method = cls.__init__

    def __init__(self: APIAction, *args, default_agent=AGENT_LOCAL, **kwargs):
        init_method(self, *args, **kwargs)
        self.default_agent = default_agent
 
    def set_default_agent(self, default_agent):
        self.default_agent = default_agent
 
    cls.__init__ = init_method
    cls.set_default_agent = set_default_agent
