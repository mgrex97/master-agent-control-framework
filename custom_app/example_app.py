from async_app_fw.base.app_manager import BaseApp
from async_app_fw.controller.handler import observe_event
from async_app_fw.event.tshark_event import tshark_event
from async_app_fw.event.tshark_event.tshark_event import EventRstpPacketIn

from product.QSW.resource.qsw_environment import QSWEnvironment


class ExampleApp(BaseApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        super().start()
        # do something

    @observe_event(tshark_event.EventTsharkPacketIn, 'tshark.rstp_packet_in')
    def get_package(self, ev):
        print('rstp pacekt in')


class HttpActionTest(BaseApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.qsw_env = QSWEnvironment()

    def start(self):
        super().start()
