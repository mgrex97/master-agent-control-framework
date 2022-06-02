import time
import inspect
import logging
from eventlet_framework.event import event
from eventlet_framework.protocol.mcp import mcp_parser_v_1_0


class EventMCPMsgBase(event.EventBase):
    def __init__(self, msg):
        self.timestamp = time.time()
        super().__init__()
        self.msg = msg


_MCP_MSG_EVENTS = {}


def _mcp_msg_name_to_ev_name(msg_name):
    return 'Event' + msg_name


def mcp_msg_to_ev(msg):
    return mcp_msg_to_ev_cls(msg.__class__)(msg)


def mcp_msg_to_ev_cls(msg_cls):
    name = _mcp_msg_name_to_ev_name(msg_cls.__name__)
    return _MCP_MSG_EVENTS[name]


def _create_mcp_msg_ev_class(msg_cls):
    name = _mcp_msg_name_to_ev_name(msg_cls.__name__)
    logging.info(f'createing mcp_event {name}')

    if name in _MCP_MSG_EVENTS:
        return

    cls = type(name, (EventMCPMsgBase,),
               dict(__init__=lambda self, msg:
                    super(self.__class__, self).__init__(msg)))
    globals()[name] = cls
    _MCP_MSG_EVENTS[name] = cls


def _create_ofp_msg_ev_from_module(ofp_parser):
    # print mod
    for _k, cls in inspect.getmembers(ofp_parser, inspect.isclass):
        if not hasattr(cls, 'cls_msg_type'):
            continue
        _create_mcp_msg_ev_class(cls)


_create_ofp_msg_ev_from_module(mcp_parser_v_1_0)

"""
for mcp_mods in mcproto.get_ofp_modules().values():
    mcp_parser = mcp_mods[1]
    # print 'loading module %s' % ofp_parser
    _create_ofp_msg_ev_from_module(ofp_parser)
"""


class EventMCPStateChange(event.EventBase):
    def __init__(self, connection, state, previous_state=None):
        super(EventMCPStateChange, self).__init__()
        self.connection = connection
        self.state = state
        self.previous_state = previous_state
