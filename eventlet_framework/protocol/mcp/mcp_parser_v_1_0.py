from eventlet_framework.protocol.mcp import mcp_v_1_0 as mcproto
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
    def __init__(self, machine):
        super().__init__(machine)
