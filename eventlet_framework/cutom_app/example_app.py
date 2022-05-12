from eventlet_framework.base.app_manager import BaseApp
from eventlet_framework.controller.handler import observe_event
from eventlet_framework.controller.tshark_controller import tshark_event
from eventlet_framework.controller.tshark_controller.tshark_event import EventRstpPacketIn

from product.QSW.resource.qsw_environment import QSWEnvironment


class ExampleApp(BaseApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @observe_event(tshark_event.EventTsharkPacketIn, 'tshark.rstp_packet_in')
    def get_package(self, ev):
        print(ev)


class HttpActionTest(BaseApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.qsw_env = QSWEnvironment()

    def start(self):
        super().start()
