import asyncio
import inspect
import logging
from types import MethodType
from async_app_fw.controller.mcp_controller.master_lib.event import ReqGetAgentConnection
from async_app_fw.base.app_manager import BaseApp
from async_app_fw.lib.hub import app_hub
from custom_app.util.constant import AsyncUtilityEventID
from custom_app.util.async_tshark import AsyncCaptureService
from custom_app.util.async_pyshark_lib.capture.async_capture import AsyncCaptureStop, DEFAULT_PACKET_CAPTURE_SIZE
from .event import EventRemoteExecute, EventRemoteCancelExecute


spawn = app_hub.spawn

class FakeCapture:
    def __init__(self, capture_size, callback=None) -> None:
        self._packets_queue = asyncio.Queue(capture_size)
        self.callback = callback

    def change_capture_size(self, capture_size):
        if capture_size == self._packets_queue.maxsize:
            return False

        del self._packets_queue

        self._packets_queue = asyncio.Queue(capture_size)

        return True

    def push_packet(self, pkt):
        queue = self._packets_queue
        if queue.full():
            logging.warning(f'Packet Queue is already full, pop out one packet from queue.')
            queue.get_nowait()

        if self.callback is None:
            queue.put_nowait(pkt)
        else:
            spawn(self.callback, pkt)

    def set_callback(self, callback):
        if not (callable(self.callback) or callback is None \
            or inspect.isfunction(callback) or inspect.ismethod(callback)):
            raise Exception(f"Input value callback should be callable or None.")

        self.callback = callback

    def reset_queue(self):
        queue = self._packets_queue
        while not queue.empty():
            queue.get_nowait()

    async def get_packet(self, timeout=None):
        get = await asyncio.wait_for(self._packets_queue.get(), timeout)
        if isinstance(get, Exception):
            raise get

        return get

    def close(self):
        if self._packets_queue.full():
            self._packets_queue.get_nowait()

        self._packets_queue.put_nowait(AsyncCaptureStop())

async def remote_execute(app: BaseApp, capture: AsyncCaptureService, args, kwargs):
    # prepare fake capture.
    capture_size = kwargs['init_var']['capture_size'] or DEFAULT_PACKET_CAPTURE_SIZE
    if (callback := kwargs['callback']) is not None:
        kwargs['callback'] = None

    fake_capture:FakeCapture = capture._fake_capture
    fake_capture.change_capture_size(capture_size)
    fake_capture.set_callback(callback)
    fake_capture.reset_queue()
    capture._capture = fake_capture

    app.send_event_to_self(EventRemoteExecute(capture, args, kwargs))
    await capture._wait_event(AsyncUtilityEventID.stop.value, timeout=None)
    fake_capture.close()

    # clean instance of capture and mcp_connection.
    capture._capture = None
    capture._mcp_connection = None

def remote_spawn_execute(method, app: BaseApp):
    def _spawn_execute_remote_compatible(self: AsyncCaptureService, *args, **kwargs):
        if getattr(self, '_mcp_connection', None) is not None:
            self._execute_task = spawn(remote_execute, app, self, args, kwargs)
        else:
            # method function, no need to pass self.
            method(*args, **kwargs)
 
    return _spawn_execute_remote_compatible

def remote_cancel_execute(method, app: BaseApp):
    def _cancel_execute_remote_compatible(self: AsyncCaptureService):
        if getattr(self, '_mcp_connection', None) is not None:
            event = EventRemoteCancelExecute(self)
            app.send_event_to_self(event)
        else:
            method()

    return _cancel_execute_remote_compatible 

def remote_start(method):
    async def _start_with_remote_compatible(self: AsyncCaptureService, *args, agent=None, **kwargs):
        if agent is not None:
            conn = await ReqGetAgentConnection.send_request(agent, timeout=5)
            self._mcp_connection = conn
 
        await method(*args, **kwargs)

    return _start_with_remote_compatible

def remote_feature_newer(app: BaseApp):
    def add_remote_feature(cls: AsyncCaptureService, *args, **kwargs):
        instance:AsyncCaptureService = object().__new__(cls)

        app.register_capture(instance)
        instance._mcp_connection = None
        instance._fake_capture = FakeCapture(DEFAULT_PACKET_CAPTURE_SIZE)
        instance._spwan_execute = MethodType(remote_spawn_execute(instance._spwan_execute, app), instance)
        instance._cancel_execute = MethodType(remote_cancel_execute(instance._cancel_execute, app), instance)
        instance.start = MethodType(remote_start(instance.start), instance)

        return instance
 
    return add_remote_feature