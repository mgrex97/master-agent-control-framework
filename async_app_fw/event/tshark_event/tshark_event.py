import logging
import time
import os

from eventlet_framework.event import event
from pyshark.packet.packet import Packet

from eventlet_framework.controller import handler
from pyshark.tshark.tshark_json import packet_from_json_packet

NAME = 'tshark_event'


def _get_json_separators():
    return ("%s  },%s" % (os.linesep, os.linesep)).encode(), ("}%s]" % os.linesep).encode(), (1 + len(os.linesep))


def extract_packet_json_from_data(data, got_first_packet=True):
    tag_start = 0
    if not got_first_packet:
        tag_start = data.find(b"{")
        if tag_start == -1:
            return None, data
    packet_separator, end_separator, end_tag_strip_length = _get_json_separators()
    found_separator = None

    tag_end = data.find(packet_separator)
    if tag_end == -1:
        # Not end of packet, maybe it has end of entire file?
        tag_end = data.find(end_separator)
        if tag_end != -1:
            found_separator = end_separator
    else:
        # Found a single packet, just add the separator without extras
        found_separator = packet_separator

    if found_separator:
        tag_end += len(found_separator) - end_tag_strip_length
        return data[tag_start:tag_end].strip().strip(b","), data[tag_end + 1:]
    return None, data


class EventRstpPacketIn(event.EventBase):
    def __init__(self, packet: Packet):
        self.packet = packet
        self.time = packet.frame_info.time


class EventTsharkPacketIn(event.EventBase):
    def __init__(self,  packet: Packet, ip=None):
        self.client_ip = ip
        self.packet = packet
        self.time = packet.frame_info.time
        self.arrive_time = time.time()


def tshark_packet_to_ev_cls(json_string, address):
    packet = packet_from_json_packet(json_string, deduplicate_fields=False)
    return EventTsharkPacketIn(packet, address)


handler.register_service(
    'eventlet_framework.controller.tshark_controller.tshark_handler')
