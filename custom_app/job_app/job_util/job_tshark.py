import asyncio
import traceback
import pyshark
from pyshark.capture.capture import Capture, packet_from_json_packet, packet_from_xml_packet
from pyshark.capture.capture import StopCapture
from async_app_fw.lib.hub import app_hub
from custom_app.job_app.job_util.job_class import JOB_DELETE, JOB_DELETED, JOB_RUN, JOB_RUNNING, JOB_STOP, JOB_STOPED, REMOTE_MATER, STATE_MAPPING, collect_handler
from custom_app.job_app.job_util.job_class import Job, ActionHandler, HandleStateChange, ObserveOutput


class CustomLiveCapture(pyshark.LiveCapture):
    def __init__(self, remote_mode=False, remote_role=None, interface=None, bpf_filter=None, display_filter=None, only_summaries=False, decryption_key=None, encryption_type='wpa-pwk', output_file=None, decode_as=None, disable_protocol=None, tshark_path=None, override_prefs=None, capture_filter=None, monitor_mode=False, use_json=False, include_raw=False, eventloop=None, custom_parameters=None, debug=False):
        super().__init__(interface, bpf_filter, display_filter, only_summaries, decryption_key, encryption_type, output_file, decode_as,
                         disable_protocol, tshark_path, override_prefs, capture_filter, monitor_mode, use_json, include_raw, eventloop, custom_parameters, debug)
        self.remote_mode = remote_mode
        self.remote_role = remote_role

    async def sniff(self, callback, timeout=None, packet_count=None):
        """Runs through all packets and calls the given callback (a function) with each one as it is read.

        If the capture is infinite (i.e. a live capture), it will run forever, otherwise it will complete after all
        packets have been read.

        Example usage:
        def print_callback(pkt):
            print(pkt)
        capture.apply_on_packets(print_callback)

        If a timeout is given, raises a Timeout error if not complete before the timeout (in seconds)
        """
        coro = self.packets_from_tshark(callback, packet_count=packet_count)
        if timeout is not None:
            coro = asyncio.wait_for(coro, timeout)
        try:
            await coro
            print('await coror after')
        except Exception as e:
            print(traceback.format_exc())

    close = Capture.close_async

    async def _get_packet_from_stream(self, stream, existing_data, got_first_packet=True, psml_structure=None):
        """A coroutine which returns a single packet if it can be read from the given StreamReader.

        :return a tuple of (packet, remaining_data). The packet will be None if there was not enough XML data to create
        a packet. remaining_data is the leftover data which was not enough to create a packet from.
        :raises EOFError if EOF was reached.
        """
        # yield each packet in existing_data
        if self.use_json:
            packet_bytes, existing_data = self._extract_packet_json_from_data(existing_data,
                                                                              got_first_packet=got_first_packet)
        else:
            packet_bytes, existing_data = self._extract_tag_from_data(
                existing_data)

        if packet_bytes:
            # return packet_bytes (original data)
            return packet_bytes, existing_data

        new_data = await stream.read(self.DEFAULT_BATCH_SIZE)
        existing_data += new_data

        if not new_data:
            # Reached EOF
            self._eof_reached = True
            raise EOFError()
        return None, existing_data

    # integrate with agent master mechanisim
    async def _go_through_packets_from_fd(self, fd, packet_callback, packet_count=None):
        """A coroutine which goes through a stream and calls a given callback for each XML packet seen in it."""
        packets_captured = 0
        self._log.debug("Starting to go through packets")

        psml_struct, data = await self._get_psml_struct(fd)

        unpack_option = {
            'use_json': self.use_json,
            'input': {}
        }

        if self.use_json is True:
            unpack_option['input']['deduplicate_fields'] = self._json_has_duplicate_keys
        else:
            unpack_option['input']['psml_structure'] = psml_struct

        while True:
            try:
                packet_bytes, data = await self._get_packet_from_stream(fd, data, got_first_packet=packets_captured > 0,
                                                                        psml_structure=psml_struct)
            except EOFError:
                self._log.debug("EOF reached")
                self._eof_reached = True
                break

            if packet_bytes:
                packets_captured += 1
                try:
                    # decode bytes first, then packet can send to master.
                    packet_callback(packet_bytes.decode(
                        encoding='utf-8'), unpack_option)
                except StopCapture:
                    self._log.debug("User-initiated capture stop in callback")
                    break

            if packet_count and packets_captured >= packet_count:
                break


TSHARK_JOB = 3


@collect_handler
@Job.register_job_type(TSHARK_JOB)
class JobTshark(Job):
    def __init__(self, tshark_options, connection=None, timeout=60, state_inform_interval=5, remote_mode=False, remote_role=None):
        super().__init__(connection, timeout, state_inform_interval, remote_mode, remote_role)
        self.tshark_options = tshark_options
        self.output_method = None

        if remote_mode is True and remote_role == REMOTE_MATER:
            return

        self.capture = CustomLiveCapture(
            **self.tshark_options, eventloop=app_hub.loop)
        self.capture.set_debug()

    def job_info_serialize(self):
        output = super().job_info_serialize()
        output['tshark_options'] = self.tshark_options
        return output

    def set_output_method(self, output_method):
        self.output_method = output_method

    @classmethod
    def create_job_by_job_info(cls, job_info, connection, remote_role=None):
        return cls(job_info['tshark_options'], connection,
                   job_info['timeout'], job_info['state_inform_interval'], job_info['remote_mode'], remote_role)

    @ActionHandler(JOB_RUN, JOB_RUNNING)
    def run(self):
        pass

    @HandleStateChange((JOB_RUN, JOB_RUNNING), JOB_STOP)
    async def tshark_running(self):
        await self.capture.sniff(self.tshark_output_preprocess)

    @ObserveOutput(JOB_RUNNING, agent_handle=False, remote_output=True)
    def tshark_output_preprocess(self, state, packet_str, unpack_info):
        packet_bytes = packet_str.encode(encoding='utf-8')

        if unpack_info['use_json'] is True:
            packet = packet_from_json_packet(
                packet_bytes, **unpack_info['input'])
        else:
            packet = packet_from_xml_packet(
                packet_bytes, **unpack_info['input'])

        if self.output_method is not None:
            tshark_info = {
                'tshark_options': self.tshark_options
            }
            if self.connection is not None:
                tshark_info['address'] = self.connection.address[0]

            self.output_method(tshark_info, packet)
        else:
            print(packet)

    @ActionHandler(JOB_STOP, JOB_STOPED, cancel_current_task=True)
    async def stop(self):
        await self.capture.close()

    @ActionHandler(JOB_DELETE, JOB_DELETED, require_before=True, cancel_current_task=True)
    async def delete(self, before=None):
        # append stop into handler_exe_queue
        if before != JOB_STOPED:
            self.stop(cancel_current_task=True)
            # append delete into handler_exe_queue
            self.delete(cancel_current_task=False)
            await asyncio.sleep(4)
        else:
            # wait output queue and exe handler queue stop.
            super().delete()
