import logging
import traceback
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from async_app_fw.controller.handler import observe_event
from async_app_fw.controller.mcp_controller.mcp_state import MC_DISCONNECT, MC_STABLE
from async_app_fw.event.mcp_event import mcp_event
from async_app_fw.event import event
from custom_app.api_action_app.exception import AgentNotExist, AuthNotExist, CantFindAgent
from custom_app.util.async_api_action import APIAction
from .constant import API_ACTION_CONTROLLER_MASTER_APP_NAME as APP_NAME
from .master_lib import event as api_event
from .master_lib.util import add_remote_feature_to_APIAction

_REQUIRED_APP = [
    'async_app_fw.controller.mcp_controller.master_handler']

spawn = app_hub.spawn
LOG = logging.getLogger(f"APP service <{APP_NAME}: ")

# Response for APIAction's remote feature.
class APIActionMasterController(BaseApp):
    APIAction_decorate_yet = False
    APIAction_ID = 0
    _EVENTS = event.get_event_from_module(api_event)

    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = APP_NAME
        self.agent_connection = {}
        self.agent_req = {}
        self.api_actions = {}
        self.xid_to_req = {}

        # Make APIAction competible with remote feature.
        add_remote_feature_to_APIAction(APIAction)

    async def close(self):
        await super().close()

        # make sure all of the api_action's connection are closed.
        for _, api_action in self.api_actions.items():
            await api_action.close()

    @classmethod
    def get_new_api_action_id(cls):
        new_id = cls.APIAction_ID
        cls.APIAction_ID = new_id + 1
        return new_id

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

    def register_api_action(self, api_action):
        LOG.info(f"Register API Action.")
        id = APIActionMasterController.get_new_api_action_id()
        api_action._ID = id
        self.api_actions[id] = api_action

    @observe_event(api_event.ReqRemoteAPILogin)
    def remote_api_action_login(self, ev: api_event.ReqRemoteAPILogin):
        agent = ev.agent
        api_action = ev.api_action

        if (conn := self.agent_connection.get(agent, None)) is None:
            exp = CantFindAgent(f"Can't find target agent <{agent}>.")
            ev.push_reply(exp)
            return

        # send login request to target agent
        msg = conn.mcproto_parser.APILogin(conn, api_action_id=api_action._ID, session_info=ev.session_info, args=ev.args, kwargs=ev.kwargs)
        conn.set_xid(msg)
        self.xid_to_req.setdefault(msg.xid, ev)
        conn.send_msg(msg)

    @observe_event(mcp_event.EventAPILoginResponse)
    def remote_login_reply_handler(self, ev):
        msg = ev.msg

        if (req_ev := self.xid_to_req.get(msg.xid, None)) is None:
            LOG.warning(f"Remote API Login reply error.")
            return

        req_ev.api_action.set_auth(msg.auth)
        req_ev.push_reply(msg.auth)

    @observe_event(mcp_event.EventAPILoginFailed)
    def remote_api_login_falied(self, ev):
        msg = ev.msg 

        if (req := self.xid_to_req.pop(msg.xid, None)) is None:
            LOG.warning(f"Can't find target login request event. xid = {msg.xid}")
            return

        req.push_reply(msg.exception)

    # Handler of APIAction's request feature.
    @observe_event(api_event.ReqSendRequestFromRemote)
    def request_from_remote(self, ev: api_event.ReqSendRequestFromRemote):
        if (conn := self.agent_connection.get(ev.agent, None)) is None: 
            ev.push_reply(AgentNotExist(f"Agent <{ev.agent}>"))
            return

        api_action: APIAction = ev.api_action

        msg = conn.mcproto_parser.APIActionRequest(conn, api_action._ID, ev.method, api_action.auth, api_action.base_url, ev.args, ev.kwargs)
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