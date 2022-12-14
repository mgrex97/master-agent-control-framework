import collections
import struct
import base64
import logging

from async_app_fw.lib import stringify
from async_app_fw.protocol.mcp.mcp_common import MCP_HEADER_PACK_STR, MCP_HEADER_SIZE

LOG = logging.getLogger(
    'eventlet_framwork.controller.mcp_controller.mcp_parser')


class McpMsgBufferdLessThanMsgLength(Exception):
    def __init__(self, buf_len, msg_len, message="Length of buffer is less then msg_len. Buffer Length:{}, Msg Length:{}", *args) -> None:
        self.message = message.format(buf_len, msg_len)
        self.msg_len = msg_len
        self.buf_len = buf_len
        super().__init__(message, *args)


class WrongMcpMsgHeader(Exception):
    pass


def header(buf):
    assert len(buf) >= MCP_HEADER_SIZE

    return struct.unpack_from(MCP_HEADER_PACK_STR, buf)


def register_msg_parser(keyword='not implement'):
    def register(msg_parser):
        _MSG_PARSERS[keyword] = msg_parser
        return msg_parser
    return register


_MSG_PARSERS = {}


def msg(connection, msg_type, msg_len, version, xid, buf):
    exp = None

    try:
        assert len(buf) >= msg_len
    except AssertionError:
        exp = McpMsgBufferdLessThanMsgLength(len(buf), len(msg_len))

    msg_parser = _MSG_PARSERS.get(version)

    try:
        msg = msg_parser(connection, msg_type, msg_len, version, xid, buf)
    except Exception as e:
        LOG.exception(
            'Encountered an error while parsing MachineControl packet from test device.')
        exp = e

    if exp:
        raise exp

    return msg


class StringifyMixin(stringify.StringifyMixin):
    _class_prefixes = ["MCP"]

    @classmethod
    def cls_from_jsondict_key(cls, k):
        obj_cls = super(StringifyMixin, cls).cls_from_jsondict_key(k)
        return obj_cls


class MCPMsgBase(object):
    def __init__(self, connection):
        self.connection = connection
        self.msg_type = None
        self.msg_len = None
        self.version = None
        self.xid = None
        self.buf = None

    def set_headers(self, msg_type, msg_len, version, xid):
        assert msg_type == self.cls_msg_type

        self.msg_type = msg_type
        self.msg_len = msg_len
        self.version = version
        self.xid = xid

    def set_xid(self, xid):
        self.xid = xid

    def set_buf(self, buf):
        self.buf = bytes(buf)

    @classmethod
    def parser(cls, connection, msg_type, msg_len, version, xid, buf):
        msg_ = cls(connection)
        msg_.set_headers(msg_type, msg_len,
                         version, xid)
        msg_.set_buf(buf)
        return msg_

    def _serialize_pre(self):
        self.version = self.connection.mcproto_parser.VERSION_ID
        self.msg_type = self.cls_msg_type
        self.buf = bytearray(MCP_HEADER_SIZE)

    def _serialize_header(self):
        # buffer length is determined after trailing data is formated.
        assert self.msg_type is not None
        assert self.buf is not None
        assert len(self.buf) >= MCP_HEADER_SIZE

        self.msg_len = len(self.buf)
        if self.xid is None:
            self.xid = 0

        struct.pack_into(MCP_HEADER_PACK_STR,
                         self.buf, 0,
                         self.msg_type, self.msg_len, self.version, self.xid)

    def _serialize_body(self):
        pass

    def serialize(self):
        self._serialize_pre()
        self._serialize_body()
        self._serialize_header()


class MsgInMsgBase(MCPMsgBase):
    @classmethod
    def _decode_value(cls, k, json_value, decode_string=base64.b64decode,
                      **additional_args):
        return cls._get_decoder(k, decode_string)(json_value,
                                                  **additional_args)


def namedtuple(typename, fields, **kwargs):
    class _namedtuple(StringifyMixin,
                      collections.namedtuple(typename, fields, **kwargs)):
        pass
    return _namedtuple


def msg_str_attr(msg_, buf, attr_list=None):
    if attr_list is None:
        attr_list = stringify.obj_attrs(msg_)
    for attr in attr_list:
        val = getattr(msg_, attr, None)
        if val is not None:
            buf += ' %s %s' % (attr, val)

    return buf
