import logging
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
    'async_app_fw.controller.mcp_controller.agent_handler']

LOG = logging.getLogger("APIAction Agent Controller")

class APIActionAgentController(BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.conn = None
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
    def login_handler(self, ev):
        spawn(self._login, ev.msg)

    async def _login(self, login_msg):
        conn = self.conn
        api_action_id = login_msg.api_action_id
        logging.info(login_msg.session_info)
        session_info = SessionInfo(**login_msg.session_info)
        args = login_msg.args
        kwargs = login_msg.kwargs
        
        try:
            # api_action hasn't existed yet. Create a new one.
            if (api_action := self.api_action.get(api_action_id, None)) is None:
                if session_info is None:
                    raise SessionInfoNotExist(f"Login into APIAction failed.")

                api_action = APIAction(session_info.base_url, session_info.login_info, auth=session_info.auth) 

                # save new api_action
                self.api_action[api_action_id] = api_action
    
            auth = await api_action.login_api(*args, **kwargs)

            msg = conn.mcproto_parser.APILoginResponse(
                conn, api_action_id, auth)

        except Exception as e:
            msg = conn.mcproto_parser.APILoginFailed(e)
            
            LOG.warning(f"API Login Failed.")
 
        msg.xid = login_msg.xid
        conn.send_msg(msg)

    @observe_event(mcp_event.EventAPIActionRequest)
    def api_action_request_handler(self, ev):
        msg = ev.msg

        # get api_action by api_action_id
        if (api_action := self.api_action.get(msg.api_action_id, None)) is None:
            api_action = APIAction(msg.base_url, None, auth=msg.auth)
            self.api_action[msg.api_action_id] = api_action

        # set auth
        api_action.set_auth(msg.auth)

        # Create a new task to send request.
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