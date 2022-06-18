import json
import struct
from async_app_fw.protocol.mcp import mcp_v_1_0 as mcproto
from async_app_fw.lib.pack_utils import msg_pack_into
from async_app_fw.protocol.mcp import mcp_parser
import logging

from async_app_fw.protocol.mcp.mcp_parser import MCPMsgBase

LOG = logging.getLogger('mcp_v_1_0')

_MSG_PARSERS = {}


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


@mcp_parser.register_msg_parser()
def msg_parser(connection, msg_type, msg_len, xid, buf):
    parser = _MSG_PARSERS.get(msg_type)
    return parser(connection, msg_type, msg_len, xid, buf)


@_register_parser
@_set_msg_type(mcproto.MCP_HELLO)
class MCPHello(MCPMsgBase):
    def __init__(self, mcp_connection, connection_id=None):
        super().__init__(mcp_connection)
        self.connection_id = connection_id

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPHello, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

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
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPJobIDWithInfo, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

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
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPJobCreateRequest, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

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
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPJobACK, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

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
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPJobStateChange, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

        (msg.job_id, msg.state_change, msg.info_len) = struct.unpack_from(
            mcproto.MCP_JOB_STATE_CHANGE_STR, msg.buf, mcproto.MCP_HEADER_SIZE)

        # decode state_chage
        msg.before = msg.state_change >> 4
        msg.after = msg.state_change & 0x0f

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_JOB_CREATE_REQUEST_SIZE

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
@_set_msg_type(mcproto.MCP_JOB_RUNNING_OUTPUT)
class MCPJobOutput(MCPJobIDWithInfo):
    pass


@_register_parser
@_set_msg_type(mcproto.MCP_JOB_DELETE_REPLY)
class MCPJobDeleteReply(MCPMsgBase):
    def __init__(self, mcp_connection, job_id):
        super().__init__(mcp_connection)
        self.job_id = job_id

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPJobDeleteReply, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

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
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPJobDeleteRequest, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

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
