from datetime import datetime
import logging

import async_app_fw.base.app_manager
from async_app_fw.controller.tshark_controller.tshark_controller import TsharkController

from async_app_fw.lib import hub
from async_app_fw.event.tshark_event import tshark_event
from async_app_fw.controller.handler import observe_event
from async_app_fw.cfg import CONF

LOG = logging.getLogger(
    'eventlent_framework.controller.tshark.tshark_controller')


def check_packet_delay(packet):
    now_time = datetime.now()
    delay = now_time.timestamp() - float(packet.sniff_timestamp)
    LOG.info(
        f"Current time: {now_time}, Packet time: {packet.sniff_time}, Delay: {delay}")


class RemoteTsharkHandler(async_app_fw.base.app_manager.BaseApp):
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = tshark_event.NAME
        self.controller = None

    def start(self):
        super().start()
        self.controller = TsharkController()
        return hub.spawn(self.controller)

    @observe_event(tshark_event.EventTsharkPacketIn, 'tshark.packet_in')
    def rstp_packet_handler(self, event: tshark_event.EventTsharkPacketIn):
        if 'stp' not in event.packet:
            return

        LOG.info(f"STP packet in. From client: {event.client_ip}")
        # self.send_event_to_observers(event, 'tshark.rstp_packet_in')
        # check_packet_delay(event.packet)

    @observe_event(tshark_event.EventTsharkPacketIn, 'tshark.packet_in')
    def vlan_packet_handler(self, event: tshark_event.EventTsharkPacketIn):
        if 'stp' not in event.packet:
            return

        LOG.info(f"STP packet in. From client: {event.client_ip}")