import logging
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.event import event
from custom_app.api_action_app.exception import AgentNotExist, AuthNotExist, CantFindAgent, LoginInfoNotExist, SessionInfoNotExist
from custom_app.util.async_api_action import APIAction, APIActionNotExist, SessionInfo
from .constant import API_ACTION_CONTROLLER_MASTER_APP_NAME as APP_NAME, AGENT_LOCAL
from .master_lib import event as api_event
from .master_lib.util import remote_request_decorator, remote_request_init_decorator

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']

spawn = app_hub.spawn
LOG = logging.getLogger(f"APP service <{APP_NAME}: ")


class APIActionMasterController(BaseApp):
    APIAction_decorate_yet = False
    _EVENTS = event.get_event_from_module(api_event)

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
            APIAction.get = remote_request_decorator(APIAction.get)
            APIAction.post = remote_request_decorator(APIAction.post)
            APIAction.put = remote_request_decorator(APIAction.put)
            APIAction.delete = remote_request_decorator(APIAction.delete)
            APIAction.patch = remote_request_decorator(APIAction.patch)
            remote_request_init_decorator(APIAction)

            self.APIAction_decorate_yet = True

    async def close(self):
        await super().close()

        # make sure all of the api_action's connection are closed.
        for _, api_action in self.api_action.items():
            await api_action.close()

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

    @observe_event(api_event.ReqGetAPIAction)
    def get_api_action(self, ev:api_event.ReqGetAPIAction):
        api_hostname = ev.api_hostname

        if (api_action := self.api_action.get(api_hostname, None)) is None:
            ev.push_reply(APIActionNotExist())
            LOG.warning(f"Can't find target({api_hostname}) APIAction.")
        
        ev.push_reply(api_action)

    @observe_event(api_event.ReqAPILoginCheck)
    def check_login(self, ev: api_event.ReqAPILoginCheck):
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

    async def _login(self, ev: api_event.ReqAPILoginCheck):
        api_hostname = ev.api_hostname
        session_info: SessionInfo = ev.session_info or self.session_info.get(ev.api_hostname)

        # api_action hasn't existed yet. Create a new one.
        if (api_action := self.api_action.get(ev.api_hostname, None)) is None:
            if session_info is None:
                ev.push_reply(SessionInfoNotExist())
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

    @observe_event(api_event.ReqSessionInit)
    def init_session_info(self, ev: api_event.ReqSessionInit):
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

    @observe_event(api_event.ReqSendRequestFromRemote)
    def request_from_remote(self, ev: api_event.ReqSendRequestFromRemote):
        if (conn := self.agent_connection.get(ev.agent, None)) is None: 
            ev.push_reply(AgentNotExist(f"Agent <{ev.agent}>"))
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