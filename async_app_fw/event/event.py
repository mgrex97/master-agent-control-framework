# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011 Isaku Yamahata <yamahata at valinux co jp>
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
from async_app_fw.controller.handler import FILTER_TYPE


def _is_event_class(cls):
    return inspect.isclass(cls) and issubclass(cls, EventBase)


def get_event_from_module(module):
    return [event for _, event in inspect.getmembers(module, _is_event_class)]


class EventBase(object):
    """
    The base of all event classes.

    A Ryu application can define its own event type by creating a subclass.
    """

    FILTER_TYPE = FILTER_TYPE

    def __init__(self):
        super(EventBase, self).__init__()


class EventRequestBase(EventBase):
    """
    The base class for synchronous request for RyuApp.send_request.
    """

    def __init__(self):
        super(EventRequestBase, self).__init__()
        self.dst = None  # app.name of provide the event.
        self.src = None
        self.sync = False
        self.reply_q = None


class EventReplyBase(EventBase):
    """
    The base class for synchronous request reply for RyuApp.send_reply.
    """

    def __init__(self, dst):
        super(EventReplyBase, self).__init__()
        self.dst = dst


class EventSocketConnecting(EventBase):
    def __init__(self, connection):
        super(EventSocketConnecting, self).__init__()
        self.connection = connection
        self.address = connection.address
