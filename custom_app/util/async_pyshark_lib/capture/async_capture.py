import asyncio
import logging
from pyshark.capture.capture import Capture, StopCapture

DEFAULT_PACKET_CAPTURE_SIZE = 200
DEFAULT_GET_PACKET_TIMEOUT = 5

class AsyncCaptureStop(Exception):
    pass

class AsyncCapture(Capture):
    def __init__(self, capture_size=DEFAULT_PACKET_CAPTURE_SIZE, log=None, display_filter=None, only_summaries=False, eventloop=None, decryption_key=None, encryption_type="wpa-pwd", output_file=None, decode_as=None, disable_protocol=None, tshark_path=None, override_prefs=None, capture_filter=None, use_json=False, include_raw=False, use_ek=False, custom_parameters=None, debug=False):
        self._log = log or logging.getLogger('Async Capture')
        super().__init__(display_filter, only_summaries, eventloop, decryption_key, encryption_type, output_file, decode_as, disable_protocol, tshark_path, override_prefs, capture_filter, use_json, include_raw, use_ek, custom_parameters, debug)
        self._capture_size = capture_size
        self._packets_queue = asyncio.Queue(capture_size)

    @property
    def _packets_from_tshark_sync(self):
        raise AttributeError()

    @property
    def apply_on_packets(self):
        raise AttributeError()

    @property
    def packets_from_tshark(self):
        raise AttributeError()

    async def push_packet(self, pkt):
        queue = self._packets_queue
        if queue.qsize() > self._capture_size:
            self._log.warning(f'Packet Queue is already full, pop out one packet from queue.')
            queue.get_nowait()

        await queue.put(pkt)

    async def get_packet(self, timeout=None):
        timeout = timeout or DEFAULT_GET_PACKET_TIMEOUT
        get = await asyncio.wait_for(self._packets_queue.get(), timeout)

        if isinstance(get, Exception):
            raise get

        return get


    async def capture_packet(self, packet_callback=None, timeout=None, packet_count=None):
        while not self._packets_queue.empty():
            self._packets_queue()

        packet_callback = packet_callback or self.push_packet
        tshark_process = await self._get_tshark_process(packet_count=packet_count)

        try:
            await asyncio.wait_for(self._go_through_packets_from_fd(\
                tshark_process.stdout, packet_callback, packet_count=packet_count), timeout)
        except asyncio.TimeoutError:
            pass

    async def _go_through_packets_from_fd(self, fd, packet_callback, packet_count=None):
        """A coroutine which goes through a stream and calls a given callback for each XML packet seen in it."""
        packets_captured = 0
        self._log.debug("Starting to go through packets")

        parser = self._setup_tshark_output_parser()
        data = b""

        while True:
            try:
                packet, data = await parser.get_packets_from_stream(fd, data,
                                                                    got_first_packet=packets_captured > 0)
            except EOFError:
                self._log.debug("EOF reached")
                self._eof_reached = True
                break

            if packet:
                packets_captured += 1
                try:
                    await packet_callback(packet)
                except StopCapture:
                    self._log.debug("User-initiated capture stop in callback")
                    break

            if packet_count and packets_captured >= packet_count:
                break

    async def get_packet(self, timeout=None):
        return await asyncio.wait_for(self._packets_queue.get(), timeout)

    async def close(self):
        self._packets_queue.put_nowait(AsyncCaptureStop())

        for process in self._running_processes.copy():
            await self._cleanup_subprocess(process)
        self._running_processes.clear()

        # Wait for all stderr handling to finish
        await asyncio.gather(*self._stderr_handling_tasks)