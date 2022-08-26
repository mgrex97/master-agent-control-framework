import traceback
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from custom_app.api_action_app.exception import SessionInfoNotExist
from custom_app.util.async_api_action import APIAction, RemoteResponse, SessionInfo
from async_app_fw.lib.hub import app_hub
from .constant import API_ACTION_CONTROLLER_AGENT_APP_NAME as APP_NAME

spawn = app_hub.spawn

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']


class APIActionAgentController(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.conn = None
        self.session_info = {}
        self.api_action = {}


    @observe_event(mcp_event.EventMCPStateChange, MC_STABLE)
    def connect_master(self, ev):
        conn = ev.connection
        self.address = conn.address[0]
        self.conn = conn

    @observe_event(mcp_event.EventMCPStateChange, MC_DISCONNECT)
    def disconnect_master(self, ev):
        self.conn = None
        self.master_address = None


    @observe_event(mcp_event.EventAPILogin)
    def check_login(self, ev):
        spawn()


    async def login(self, api_hostname, session_info:SessionInfo, xid=None):
        conn = self.conn
        try:
            # api_action hasn't existed yet. Create a new one.
            if (api_action := self.api_action.get(api_hostname, None)) is None:
                if session_info is None:
                    raise SessionInfoNotExist
    
                api_action = APIAction(session_info.base_url, session_info.login_info, session_info.auth)
            
                # save new api_action
                self.api_action[api_hostname] = api_action
    
            await api_action.login_api(session_info.login_info)

        except Exception as e:
            # send exception info
            if xid is not None and conn is not None:
                msg = conn.mcproto_parser.APILoginFailed(e)
                msg.xid = xid
                conn.send_msg(msg)

            raise e

        # get Session info
        session_info = api_action.get_APIAction(api_hostname)
        # update
        self.session_info[api_hostname] = session_info

        if xid is not None and conn is not None:
            # send login result to master
            msg = conn.mcproto_parser.APILoginResponse(
                conn, api_hostname, session_info)
            msg.xid = xid
            conn.send_msg(msg)

    @observe_event(mcp_event.EventAPIActionRequest)
    def api_action_request_handler(self, ev):
        msg = ev.msg

        # get api_action by hostname
        if (api_action := self.api_action.get(msg.hostname, None)) is None:
            api_action = APIAction(msg.base_url, None, auth=msg.auth)
            self.api_action[msg.hostname] = api_action

        # set auth
        api_action.set_auth(msg.auth)

        # Create a new task to handle request.
        spawn(self.send_api_request, msg.xid, api_action, msg.method, msg.args, msg.kwargs)

    async def send_api_request(self, xid, api_action, method_name, args, kwargs):
        conn = self.conn
        try:
            resp = await api_action.method_set[method_name](*args, **kwargs)
            msg = conn.mcproto_parser.APIActionResponse(conn, RemoteResponse(resp))
            msg.xid = xid

            conn.send_msg(msg)
        except Exception as e:
            # send exception back
            print(traceback.format_exc())