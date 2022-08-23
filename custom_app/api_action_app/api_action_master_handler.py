import inspect
import logging
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.event.async_event import EventAsyncRequestBase
from custom_app.api_action_app.exception import AgentNotExist, AuthNotExist, CantFindAgent, LoginInfoNotExist, SessionInfoNotExist
from custom_app.util.async_api_action import APIAction, APIActionNotExist, SessionInfo

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']

spawn = app_hub.spawn

APP_NAME = 'api_action_master_handler'
DEFAULT_LOGIN_TIMEOUT = 5
DEFAULT_SESSION_INIT_TIMEOUT = 5
REMOTE_REQUEST_DEFAULT_TIMEOUT = 300 # 5 mins
AGENT_LOCAL = 'local'

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

LOG = logging.getLogger(f"APP service <{APP_NAME}: ")

# This decorator make request methods(post,get...) have ability to execute request on agent.
def _remote_request_decorator(fun):
    method = getattr(fun, '_method_name')

    async def _remote_send_request(self, *args, agent=AGENT_LOCAL, timeout=REMOTE_REQUEST_DEFAULT_TIMEOUT,method_name=method, **kwargs):
        if agent == AGENT_LOCAL:
            res = await fun(self, *args, *kwargs)
        else:
            # send request (Event) to APIActionMasterController to deal with remote request.
            res = await ReqSendRequestFromRemote.send_request(agent, method_name, self, *args, timeout=timeout, **kwargs)
        
        return res

    return _remote_send_request


class APIActionMasterController(BaseApp):
    APIAction_decorate_yet = False

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.agent_connection = {}
        self.agent_req = {}
        # {api_host_key: session_info}
        self.session_info = {}
        self.api_action = {}
        self.api_action_to_name = {}
        self.xid_to_req = {}
        # maybe agent host name can be loaded from config file.
        # self.agent_name_map = {}

        # decorate APIAction's method(get,post...) function.
        # For remote feature.
        if self.APIAction_decorate_yet is False:
            APIAction.get = _remote_request_decorator(APIAction.get)

            self.APIAction_decorate_yet = True

    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def agent_join(self, ev):
        conn = ev.connection
        address = conn.address[0]

        self.agent_connection[address] = conn
        self.agent_req = {address: set()}

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def agent_leave(self, ev):
        conn = ev.connection
        address = conn.address[0]

        self.agent_connection.pop(address)
        req_set = self.agent_req.pop(address, set())

        """
        for req in req_set:
            try:
                # push exception.
                req.push_reply()
            except Exception as e:
                pass
        """

    @observe_event(ReqGetAPIAction)
    def get_api_action(self, ev:ReqGetAPIAction):
        api_hostname = ev.api_hostname

        if (api_action := self.api_action.get(api_hostname, None)) is None:
            ev.push_reply(APIActionNotExist())
            LOG.warning(f"Can't find target({api_hostname}) APIAction.")
        
        ev.push_reply(api_action)

    @observe_event(ReqAPILoginCheck)
    def check_login(self, ev: ReqAPILoginCheck):
        api_hostname = ev.api_hostname
        agent_address= ev.agent_address

        if agent_address == AGENT_LOCAL:
            # create login task at local side.
            spawn(self._login, ev) 
            return

        if (conn := self.agent_connection.get(agent_address, None)) is None:
            exp = CantFindAgent()
            ev.push_reply(exp)
            return

        # send requestion to target connection

    def remote_login(self, conn):
        pass

    def remote_login_reply_handler(self, ev):
        pass

    async def _login(self, ev: ReqAPILoginCheck):
        api_hostname = ev.api_hostname
        session_info: SessionInfo = ev.session_info or self.session_info.get(ev.api_hostname)

        # api_action hasn't existed yet. Create a new one.
        if (api_action := self.api_action.get(ev.api_hostname, None)) is None:
            if session_info is None:
                ReqAPILoginCheck.push_reply(SessionInfoNotExist())
                return

            api_action = APIAction(session_info.base_url, session_info.login_info, session_info.auth)

            # update session_info
            self.session_info[api_hostname] = session_info
 
        try:
            await api_action.login_api(session_info.login_info)
        except Exception as e:
            ev.push_reply(e)
            return
        
        # take out the new auth and apply to session_info
        session_info.auth = api_action.auth

        # save api_action
        self.api_action[api_hostname] = api_action
        self.api_action_to_name[api_action] = api_hostname
        ev.push_reply(api_action)

    @observe_event(ReqSessionInit)
    def init_session_info(self, ev: ReqSessionInit):
        session_info = ev.session_info

        if self.session_info.get(session_info.api_hostname, None) is not None:
            ev.push_reply(SessionInfoNotExist())
            return

        self.session_info[session_info.api_hostname] = session_info
        ev.push_reply(session_info)

    @observe_event(mcp_event.EventAPILoginFailed)
    def remote_api_login_falied(self, ev):
        pass

    def get_api_action_by_hostname(self, hostname):
        try:
            api_action = self.api_action[hostname]
        except KeyError as e:
            raise APIActionNotExist()

        return api_action

    @observe_event(mcp_event.EventAPILoginResponse)
    def remote_api_exception_handler(self, ev):
        pass

    @observe_event(ReqSendRequestFromRemote)
    def request_from_remote(self, ev: ReqSendRequestFromRemote):
        if (conn := self.agent_connection.get(ev.agent, None)) is None: 
            ev.push_reply(AgentNotExist())
            return

        api_action: APIAction = ev.api_action
        hostname = self.api_action_to_name[api_action]

        msg = conn.mcproto_parser.APIActionRequest(conn, hostname, ev.method, api_action.auth, api_action.base_url, ev.args, ev.kwargs)
        conn.set_xid(msg)

        self.xid_to_req[msg.xid] = ev
        conn.send_msg(msg)

    @observe_event(mcp_event.EventAPIActionResponse)
    def remote_api_response(self, ev):
        msg = ev.msg 

        if (req := self.xid_to_req.pop(msg.xid, None)) is None:
            LOG.warning(f"Can't find target request, xid = {msg.xid}")
            return

        req.push_reply(msg.response)