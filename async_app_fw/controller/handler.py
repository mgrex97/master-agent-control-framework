# Copyright (C) 2011-2014 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011, 2012 Isaku Yamahata <yamahata at valinux co jp>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import inspect
import logging
import sys

LOG = logging.getLogger('async_app_fw.controller.handler')

# just represent OF datapath state. datapath specific so should be moved.


class DuplicateHandler(Exception):
    pass


HANDLER_FILTER = {}


def register_handler_filter(filter_type):
    def register(handler_filter):
        if filter_type in HANDLER_FILTER:
            raise DuplicateHandler(
                f'Filter type {filter_type} is already existed. \n Current <{filter_type}:{HANDLER_FILTER[filter_type].__name__}>')

        HANDLER_FILTER[filter_type] = handler_filter
        return handler_filter
    return register


FILTER_TYPE = 1


@register_handler_filter(FILTER_TYPE)
def default_handler_filter(handler, ev, ev_cls, state):
    if state is None:
        return True

    if not hasattr(handler, 'callers') or ev_cls not in handler.callers:
        # dynamically registered handlers does not have
        # h.callers element for the event.
        return True

    states = handler.callers[ev_cls].ev_types
    if not states:
        # empty states means all states
        return True
    return state in states


class _Caller(object):
    """Describe how to handle an event class.
    """

    def __init__(self, ev_types, ev_source):
        """Initialize _Caller.

        :param ev_type: A list of states or a state, in which this
                            is in effect.
                            None and [] mean all states.
        :param ev_source: The module which generates the event.
                          ev_cls.__module__ for set_ev_cls.
                          None for set_ev_handler.
        """
        self.ev_types = ev_types
        self.ev_source = ev_source


# should be named something like 'observe_event'
def observe_event(ev_cls, ev_types=None):
    def _set_ev_cls_dec(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(ev_cls):
            handler.callers[e] = _Caller(_listify(ev_types), e.__module__)
        return handler
    return _set_ev_cls_dec


def observe_event_from_self(ev_cls, ev_types):
    def _observe_event_from_self(handler):
        frame = inspect.currentframe()
        assert frame is not None
        m_name = frame.f_back.f_globals['__name__']

        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(ev_cls):
            handler.callers[e] = _Caller(_listify(ev_types), m_name)
        return handler
    return _observe_event_from_self


def observe_event_with_specific_src(ev_cls_with_src, ev_types=None):
    def _observe_event_with_specific_src(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}

        for _, data in enumerate(_listify(ev_cls_with_src)):
            e = data[0]
            src_module = data[1]

            handler.callers[e] = _Caller(_listify(ev_types), src_module)
        return handler
    return _observe_event_with_specific_src


def observe_event_without_event_source(ev_cls, ev_types=None):
    def _set_ev_cls_dec(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(ev_cls):
            handler.callers[e] = _Caller(_listify(ev_types), None)
        return handler
    return _set_ev_cls_dec


def _has_caller(meth):
    return hasattr(meth, 'callers')


def _listify(may_list):
    if may_list is None:
        may_list = []
    if not isinstance(may_list, list):
        may_list = [may_list]
    return may_list


def register_instance(i):
    for _k, m in inspect.getmembers(i, inspect.ismethod):
        # LOG.debug('instance %s k %s m %s', i, _k, m)
        if _has_caller(m):
            for ev_cls, c in m.callers.items():
                i.register_handler(ev_cls, m)


def _is_method(f):
    return inspect.isfunction(f) or inspect.ismethod(f)


def get_dependent_services(cls):
    services = []
    # get service name from cls's method.
    for _k, m in inspect.getmembers(cls, _is_method):
        if _has_caller(m):
            for ev_cls, c in m.callers.items():
                service = getattr(sys.modules[ev_cls.__module__],
                                  '_SERVICE_NAME', None)
                if service:
                    # avoid cls that registers the own events (like
                    # ofp_handler)
                    if cls.__module__ != service:
                        services.append(service)

    m = sys.modules[cls.__module__]
    # get service name from attr _REQUIRED_APP.
    services.extend(getattr(m, '_REQUIRED_APP', []))
    # filter duplicated service name from serices list.
    services = list(set(services))
    return services


def register_service(service):
    """
    Register the ryu application specified by 'service' as
    a provider of events defined in the calling module.

    If an application being loaded consumes events (in the sense of
    set_ev_cls) provided by the 'service' application, the latter
    application will be automatically loaded.

    This mechanism is used to e.g. automatically start ofp_handler if
    there are applications consuming OFP events.
    """
    frame = inspect.currentframe()
    if frame is not None:
        m_name = frame.f_back.f_globals['__name__']
        m = sys.modules[m_name]
        m._SERVICE_NAME = service
