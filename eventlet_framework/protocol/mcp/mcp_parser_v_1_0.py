import struct
from eventlet_framework.protocol.mcp import mcp_v_1_0 as mcproto
from eventlet_framework.lib.pack_utils import msg_pack_into
from eventlet_framework.protocol.mcp import mcp_parser
import logging

from eventlet_framework.protocol.mcp.mcp_parser import MCPMsgBase

LOG = logging.getLogger(
    'eventlet_framwork.controller.mcp_controller.mcp_v_1_0')

_MSG_PARSERS = {}


def _set_msg_type(msg_type):
    '''Annotate corresponding OFP message type'''
    def _set_cls_msg_type(cls):
        cls.cls_msg_type = msg_type
        return cls
    return _set_cls_msg_type


def _register_parser(cls):
    '''class decorator to register msg parser'''
    assert cls.cls_msg_type is not None
    assert cls.cls_msg_type not in _MSG_PARSERS
    _MSG_PARSERS[cls.cls_msg_type] = cls.parser
    return cls


@mcp_parser.register_msg_parser()
def msg_parser(machine, msg_type, msg_len, xid, buf):
    parser = _MSG_PARSERS.get(msg_type)
    return parser(machine, msg_type, msg_len, xid, buf)


def _set_msg_reply(msg_reply):
    def _set_cls_msg_reply(cls):
        cls.cls_msg_reply = msg_reply
        return cls
    return _set_cls_msg_reply


@_register_parser
@_set_msg_type(mcproto.MCP_HELLO)
class MCPHello(MCPMsgBase):
    def __init__(self, mcp_connection):
        super().__init__(mcp_connection)


@_register_parser
@_set_msg_type(mcproto.MCP_EXECUTE_COMMAND_REQUEST)
class MCPExecuteCommandRequest(MCPMsgBase):
    def __init__(self, mcp_connection, job_id=None, command=None):
        super().__init__(mcp_connection)
        self.job_id = job_id
        self.command = command

    @classmethod
    def parser(cls, mcp_connection, msg_type, msg_len, xid, buf):
        msg = super(MCPExecuteCommandRequest, cls).parser(
            mcp_connection, msg_type, msg_len, xid, buf)

        (msg.job_id,
         msg.cmd_len) = struct.unpack_from(
            mcproto.MCP_EXECUTE_COMMAND_REQUEST_STR,
            msg.buf, mcproto.MCP_HEADER_SIZE
        )

        offset = mcproto.MCP_HEADER_SIZE + mcproto.MCP_EXECUTE_COMMAND_REQUEST_SIZE

        msg.cmd_bytes = msg.buf[offset:]
        if msg.cmd_len < len(msg.cmd_bytes):
            msg.cmd_bytes = msg.cmd_bytes[:msg.cmd_len]

        msg.command = msg.cmd_bytes.decode(encoding='utf-8')

        return msg

    def serialize(self):
        self.cmd_bytes = self.command.encode('utf-8')
        self.cmd_len = len(self.cmd_bytes)

        return super().serialize()

    def _serialize_body(self):
        assert self.job_id is not None
        assert self.cmd_len is not None
        assert self.cmd_bytes is not None

        msg_pack_into(mcproto.MCP_EXECUTE_COMMAND_REQUEST_STR,
                      self.buf, mcproto.MCP_HEADER_SIZE, self.job_id, self.cmd_len)

        self.buf.extend(self.cmd_bytes)
