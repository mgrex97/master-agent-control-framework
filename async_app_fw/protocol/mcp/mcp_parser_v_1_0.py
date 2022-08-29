import dataclasses
import aiohttp
import json
import struct
import pickle
from async_app_fw.protocol.mcp import mcp_v_1_0 as mcproto
from async_app_fw.lib.pack_utils import msg_pack_into
from async_app_fw.protocol.mcp import mcp_parser
import logging

from async_app_fw.protocol.mcp.mcp_parser import MCPMsgBase

LOG = logging.getLogger('mcp_v_1_0')

_MSG_PARSERS = {}

VERSION_ID = 1


def _set_msg_type(msg_type):
    '''Annotate corresponding MCP message type'''
    def _set_cls_msg_type(cls):
        cls.cls_msg_type = msg_type
        return cls
    return _set_cls_msg_type


def _set_msg_reply(msg_reply):
    '''Annotate MCP reply message class'''
    def _set_cls_msg_reply(cls):
        cls.cls_msg_reply = msg_reply
        return cls
    return _set_cls_msg_reply


def _register_parser(cls):
    '''class decorator to register msg parser'''
    assert cls.cls_msg_type is not None
    assert cls.cls_msg_type not in _MSG_PARSERS
    _MSG_PARSERS[cls.cls_msg_type] = cls.parser
    return cls


@mcp_parser.register_msg_parser(keyword=VERSION_ID)
def msg_parser(connection, msg_type, msg_len, version, xid, buf):
    assert version == VERSION_ID
    parser = _MSG_PARSERS.get(msg_type)
    return parser(connection, msg_type, msg_len, version, xid, buf)


@_register_parser
@_set_msg_type(mcproto.MCP_HELLO)
class MCPHello(MCPMsgBase):
    def __init__(self, mcp_connection, connection_id=None):
        super().__init__(mcp_connection)
        self.connection_id = connection_id

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPHello, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.connection_id, ) = struct.unpack_from(
            mcproto.MCP_HELLO_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        return msg

    def _serialize_body(self):
        assert self.connection_id is not None

        msg_pack_into(mcproto.MCP_HELLO_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.connection_id)


class MCPJobIDWithInfo(MCPMsgBase):
    def __init__(self, mcp_connection, job_id=None, job_info=None):
        super().__init__(mcp_connection)
        self.job_id = job_id
        self.job_info = job_info

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobIDWithInfo, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id, msg.job_info_len) = struct.unpack_from(
            mcproto.MCP_JOB_ID_WITH_INFO_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_JOB_ID_WITH_INFO_SIZE

        msg.job_info_bytes = msg.buf[offset:]
        if msg.job_info_len < len(msg.job_info_bytes):
            msg.job_info_bytes = msg.job_info_bytes[:msg.job_info_len]

        msg.job_info_str = msg.job_info_bytes.decode(encoding='utf-8')
        msg.job_info = json.loads(msg.job_info_str)

        return msg

    def serialize(self):
        self.job_info_str = json.dumps(self.job_info)
        self.job_info_bytes = self.job_info_str.encode('utf-8')
        self.job_info_len = len(self.job_info_bytes)

        return super().serialize()

    def _serialize_body(self):
        assert self.job_id is not None
        assert self.job_info_bytes is not None
        assert self.job_info_len is not None

        msg_pack_into(mcproto.MCP_JOB_ID_WITH_INFO_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id, self.job_info_len)

        self.buf.extend(self.job_info_bytes)


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_CREATE_REPLY)
class MCPJobCreateReply(MCPJobIDWithInfo):
    pass


@_set_msg_reply(MCPJobCreateReply)
@_register_parser
@_set_msg_type(mcproto.MCP_JOB_CREATE_REQUEST)
class MCPJobCreateRequest(MCPMsgBase):
    _JOB_TYPES = {}

    def __init__(self, mcp_connection, timeout=0, job_info=None):
        super().__init__(mcp_connection)
        self.timeout = timeout
        self.job_info = job_info

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobCreateRequest, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.timeout, msg.job_info_len) = struct.unpack_from(
            mcproto.MCP_JOB_CREATE_REQUEST_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_JOB_CREATE_REQUEST_SIZE

        msg.job_info_bytes = msg.buf[offset:]
        if msg.job_info_len < len(msg.job_info_bytes):
            msg.job_info_bytes = msg.job_info_bytes[:msg.job_info_len]

        msg.job_info_str = msg.job_info_bytes.decode(encoding='utf-8')
        msg.job_info = json.loads(msg.job_info_str)

        return msg

    def serialize(self):
        if isinstance(self.job_info, dict):
            self.job_info_str = json.dumps(self.job_info)
        else:
            self.job_info_str = self.job_info

        self.job_info_bytes = self.job_info_str.encode('utf-8')
        self.job_info_len = len(self.job_info_bytes)

        return super().serialize()

    def _serialize_body(self):
        assert self.timeout is not None
        assert self.job_info_len is not None
        assert self.job_info_bytes is not None

        msg_pack_into(mcproto.MCP_JOB_CREATE_REQUEST_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.timeout, self.job_info_len)

        self.buf.extend(self.job_info_bytes)


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_ACK)
class MCPJobACK(MCPMsgBase):
    def __init__(self, mcp_connection, job_id=None):
        super().__init__(mcp_connection)
        self.job_id = job_id

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobACK, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id, ) = struct.unpack_from(
            mcproto.MCP_JOB_ACK_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        return msg

    def _serialize_body(self):
        assert self.job_id is not None

        msg_pack_into(mcproto.MCP_JOB_ACK_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id)


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_STATE_CHANGE)
class MCPJobStateChange(MCPMsgBase):
    def __init__(self, mcp_connection, job_id=None, before=None, after=None, info=None):
        super().__init__(mcp_connection)
        self.job_id = job_id
        self.before = before
        self.after = after
        # maximum size of info: 1024 bytes
        self.info = info
        self.info_len = None

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobStateChange, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id, msg.state_change, msg.info_len) = struct.unpack_from(
            mcproto.MCP_JOB_STATE_CHANGE_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        # decode state_chage
        msg.before = msg.state_change >> 4
        msg.after = msg.state_change & 0x0f

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_JOB_STATE_CHANGE_SIZE

        # there is no info.
        if msg.info_len == 0:
            msg.info = None
            msg.info_len = 0
            return msg

        # retrive job info
        msg.info_bytes = msg.buf[offset:]

        if msg.info_len < len(msg.info_bytes):
            msg.info_bytes = msg.info_bytes[:msg.info_len]

        # decode byte and load json.
        msg.info = json.loads(msg.info_bytes.decode(encoding='utf-8'))

        return msg

    def _serialize_body(self):
        assert self.job_id is not None
        assert self.before is not None
        assert self.after is not None
        if self.info is None:
            self.info = ''

        self.info_bytes = json.dumps(self.info).encode(encoding='utf-8')
        self.info_len = len(self.info_bytes)
        self.state_change = (self.before << 4) | self.after

        msg_pack_into(mcproto.MCP_JOB_STATE_CHANGE_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id, self.state_change, self.info_len)

        self.buf.extend(self.info_bytes)


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_OUTPUT)
class MCPJobOutput(MCPMsgBase):
    def __init__(self, mcp_connection, job_id=None, state=None, info=None):
        super().__init__(mcp_connection)
        self.job_id = job_id
        self.state = state
        # maximum size of info: 1024 bytes
        self.info = info
        self.info_len = None

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobOutput, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id, msg.state, msg.info_len) = struct.unpack_from(
            mcproto.MCP_JOB_STATE_CHANGE_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_JOB_OUTPUT_SIZE

        # there is no info.
        if msg.info_len == 0:
            msg.info = None
            msg.info_len = 0
            return msg

        # retrive job info
        msg.info_bytes = msg.buf[offset:]

        if msg.info_len < len(msg.info_bytes):
            msg.info_bytes = msg.info_bytes[:msg.info_len]

        # decode byte and load json.
        msg.info = json.loads(msg.info_bytes.decode(encoding='utf-8'))

        return msg

    def _serialize_body(self):
        assert self.job_id is not None
        assert self.state is not None
        if self.info is None:
            self.info = ''

        self.info_bytes = json.dumps(self.info).encode(encoding='utf-8')
        self.info_len = len(self.info_bytes)

        msg_pack_into(mcproto.MCP_JOB_OUTPUT_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id, self.state, self.info_len)

        self.buf.extend(self.info_bytes)


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_DELETE_REPLY)
class MCPJobDeleteReply(MCPMsgBase):
    def __init__(self, mcp_connection, job_id):
        super().__init__(mcp_connection)
        self.job_id = job_id

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobDeleteReply, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id) = struct.unpack_from(
            mcproto.MCP_JOB_DELETE_REPLY_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        return msg

    def _serialize_body(self):
        assert self.job_id is not None

        msg_pack_into(mcproto.MCP_JOB_DELETE_REPLY_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id)


@_set_msg_reply(MCPJobDeleteReply)
@_register_parser
@_set_msg_type(mcproto.MCP_JOB_DELETE_REQUEST)
class MCPJobDeleteRequest(MCPMsgBase):
    def __init__(self, mcp_connection, job_id):
        super().__init__(mcp_connection)
        self.job_id = job_id

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobDeleteRequest, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id) = struct.unpack_from(
            mcproto.MCP_JOB_DELETE_REQUEST_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        return msg

    def _serialize_body(self):
        assert self.job_id is not None

        msg_pack_into(mcproto.MCP_JOB_DELETE_REQUEST_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id)


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_DELETE_ALL)
class MCPJobDeleteAll(MCPMsgBase):
    pass


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_FEATURE_EXE)
class MCPJobFeatureExe(MCPMsgBase):
    def __init__(self, mcp_connection, job_id=None, state=None, info=None):
        super().__init__(mcp_connection)
        self.job_id = job_id
        self.state = state
        # maximum size of info: 1024 bytes
        self.info = info
        self.info_len = None

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobFeatureExe, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.job_id, msg.state, msg.info_len) = struct.unpack_from(
            mcproto.MCP_JOB_FEATURE_EXE_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_JOB_FEATURE_EXE_SIZE

        # there is no info.
        if msg.info_len == 0:
            msg.info = None
            msg.info_len = 0
            return msg

        # retrive job info
        msg.info_bytes = msg.buf[offset:]

        if msg.info_len < len(msg.info_bytes):
            msg.info_bytes = msg.info_bytes[:msg.info_len]

        # decode byte and load json.
        msg.info = json.loads(msg.info_bytes.decode(encoding='utf-8'))

        return msg

    def _serialize_body(self):
        assert self.job_id is not None
        assert self.state is not None
        if self.info is None:
            self.info = ''

        self.info_bytes = json.dumps(self.info).encode(encoding='utf-8')
        self.info_len = len(self.info_bytes)

        msg_pack_into(mcproto.MCP_JOB_FEATURE_EXE_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id, self.state, self.info_len)

        self.buf.extend(self.info_bytes)

class APIActionException(MCPMsgBase):
    def __init__(self, mcp_connection, exception=None):
        super().__init__(mcp_connection)
        if not isinstance(exception, Exception):
            raise ValueError(f'Input variable exception should be instance of class Exception.')

        self.info_len = None
        self.exception = exception
        self.exception_bytes = None
        # maximum size of info: 1024 bytes

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(MCPJobStateChange, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.info_len, ) = struct.unpack_from(
            mcproto.API_ACTION_EXCEPTION_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.API_ACTION_EXCEPTION_SIZE

        # retrive exception byte data
        msg.exception_bytes = msg.buf[offset:]

        if msg.info_len < len(msg.exception_bytes):
            msg.exception_bytes = msg.exception_bytes[:msg.info_len]

        # decode pickle byte
        msg.exception = pickle.loads(msg.exception_bytes)

        return msg

    def _serialize_body(self):
        assert self.exception is not None

        self.excpetion_bytes = pickle.dumps(self.exception)
        self.info_len = len(self.exception_bytes)

        msg_pack_into(mcproto.API_ACTION_EXCEPTION_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.info_len)

        self.buf.extend(self.exception_bytes)

@_register_parser
@_set_msg_type(mcproto.API_ACTION_LOGIN)
class APILogin(MCPMsgBase):
    def __init__(self, mcp_connection, api_action_id=None, session_info=None, args=None, kwargs=None):
        super().__init__(mcp_connection)
        self.api_action_id = api_action_id
        self.session_info = session_info
        self.args = args
        self.kwargs = kwargs
        self.len = None
        # maximum size of info: 1024 bytes

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(APILogin, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.api_action_id, msg.len, ) = struct.unpack_from(
            mcproto.API_ACTION_LOGIN_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.API_ACTION_LOGIN_SIZE

        # retrive byte data
        json_bytes = msg.buf[offset:]

        if msg.len < len(json_bytes):
            json_bytes = json_bytes[:msg.len]

        # decode json
        data = json.loads(json_bytes.decode('utf-8'))

        msg.session_info = data['session_info']
        msg.args = data['args']
        msg.kwargs = data['kwargs']

        return msg

    def _serialize_body(self):
        assert self.api_action_id is not None

        data_bytes = json.dumps({
            'session_info': dataclasses.asdict(self.session_info),
            'args': self.args,
            'kwargs': self.kwargs
        }).encode('utf-8')

        self.len = len(data_bytes)

        msg_pack_into(mcproto.API_ACTION_LOGIN_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.api_action_id, self.len)

        self.buf.extend(data_bytes)


@_register_parser
@_set_msg_type(mcproto.API_ACTION_LOGIN_FAILED)
class APILoginFailed(MCPMsgBase):
    def __init__(self, mcp_connection, exception=None):
        super().__init__(mcp_connection)

        if not isinstance(exception, Exception):
            raise TypeError()

        self.exception = exception
        self.data_len = None

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(APIActionException, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)
        
        (msg.data_len, ) = struct.unpack_from(mcproto.API_ACTION_LOGIN_FAILED_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.API_ACTION_LOGIN_FAILED_SIZE

        data_bytes = msg.buf[offset:]

        if msg.data_len < len(data_bytes):
            data_bytes = data_bytes[:msg.data_len]

        msg.exception = pickle.loads(data_bytes)

        return msg

    def _serialize_body(self):
        assert self.exception is not None

        data_bytes = pickle.dumps(self.exception)
        self.data_len = len(data_bytes)

        msg_pack_into(mcproto.API_ACTION_LOGIN_FAILED_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.data_len)

        self.buf.extend(data_bytes)

@_register_parser
@_set_msg_type(mcproto.API_ACTION_LOGIN_RESPONSE)
class APILoginResponse(MCPMsgBase):
    def __init__(self, mcp_connection, api_action_id=None, auth=None):
        super().__init__(mcp_connection)
        self.api_action_id = api_action_id
        self.auth = auth
        self.data_len = None
        # maximum size of info: 1024 bytes

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(APILoginResponse, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.api_action_id, msg.data_len, ) = struct.unpack_from(
            mcproto.API_ACTION_LOGIN_RESPONSE_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.API_ACTION_LOGIN_RESPONSE_SIZE

        auth_bytes = msg.buf[offset:]

        if msg.data_len < len(auth_bytes):
            auth_bytes = auth_bytes[:msg.data_len]

        # decode json
        msg.auth = auth_bytes.decode('utf-8')

        return msg

    def _serialize_body(self):
        assert self.api_action_id is not None
        assert isinstance(self.auth, str)

        auth_bytes = self.auth.encode('utf-8')

        self.data_len = len(auth_bytes)

        msg_pack_into(mcproto.API_ACTION_LOGIN_RESPONSE_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.api_action_id, self.data_len)

        self.buf.extend(auth_bytes)


@_register_parser
@_set_msg_type(mcproto.API_ACTION_REQUEST)
class APIActionRequest(MCPMsgBase):
    method_to_int= {
        'post': 0,
        'get': 1,
        'put': 2,
        'delete': 3,
        'patch': 4
    }

    # reverse
    int_to_method = {key:method for method, key in method_to_int.items()} 

    def __init__(self, mcp_connection, api_action_id=None, method=None, auth=None, base_url=None, args=None, kwargs=None):
        super().__init__(mcp_connection)
        self.api_action_id = api_action_id
        self.method = method
        self.auth = auth
        self.base_url = base_url
        self.args = args
        self.kwargs = kwargs
        self.len = None
        # maximum size of info: 1024 bytes

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(APIActionRequest, cls).parser(
            mcp_connection, msg_type, msg_len, version, xid, buf)

        (method, msg.api_action_id, msg.len, ) = struct.unpack_from(
            mcproto.API_ACTION_REQUEST_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        # decode method
        msg.method = cls.int_to_method[method]
        offset = mcproto.MCP_HEADER_SIZE + mcproto.API_ACTION_REQUEST_SIZE

        # retrive exception byte data
        json_bytes = msg.buf[offset:]

        if msg.len < len(json_bytes):
            json_bytes = json_bytes[:msg.len]

        # decode json
        data = json.loads(json_bytes.decode('utf-8'))

        msg.base_url = data['base_url']
        msg.auth = data['auth']
        msg.args = data['args']
        msg.kwargs = data['kwargs']

        return msg

    def _serialize_body(self):
        assert self.method is not None

        data_bytes = json.dumps({
            'auth': self.auth,
            'base_url': self.base_url,
            'args': self.args,
            'kwargs': self.kwargs
        }).encode('utf-8')

        self.len = len(data_bytes)

        method = self.method_to_int[self.method]
        msg_pack_into(mcproto.API_ACTION_REQUEST_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, method, self.api_action_id, self.len)

        self.buf.extend(data_bytes)

@_register_parser
@_set_msg_type(mcproto.API_ACTION_RESPONSE)
class APIActionResponse(MCPMsgBase):
    def __init__(self, mcp_connection, response: aiohttp.ClientResponse = None):
        super().__init__(mcp_connection)
        self.response = response
        self.len = None

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, version, xid, buf):
        msg = super(APIActionResponse, cls).parser(
        mcp_connection, msg_type, msg_len, version, xid, buf)

        (msg.len, ) = struct.unpack_from(
            mcproto.API_ACTION_RESPONSE_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        offset = mcproto.MCP_HEADER_SIZE + mcproto.API_ACTION_RESPONSE_SIZE

        resp_bytes = msg.buf[offset:]

        if msg.len < len(resp_bytes):
            resp_bytes = resp_bytes[:msg.len]

        # decode json
        msg.response = pickle.loads(resp_bytes)

        return msg

    def _serialize_body(self):
        assert self.response is not None

        resp_bytes = pickle.dumps(self.response)
        self.len = len(resp_bytes)

        msg_pack_into(mcproto.API_ACTION_RESPONSE_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.len)

        self.buf.extend(resp_bytes)