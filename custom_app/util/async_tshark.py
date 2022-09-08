from abc import ABC
import logging
from asyncio import CancelledError, TimeoutError as asyncTimeoutError


from .async_pyshark_lib.capture.async_live_capture import AsyncLiveCapture as _AsyncLiveCapture
from .async_pyshark_lib.capture.async_capture import AsyncCapture as _AsyncCapture
from .async_utility import AsyncUtility, check_event
from .constant import AsyncUtilityEventID as EventID

LIVE_CAPTURE_DEFAULT_CAPTURE_TIMEOUT = 30

class AsyncCaptureService(AsyncUtility, ABC):
    def __init__(self, name='...', exe_timeout=LIVE_CAPTURE_DEFAULT_CAPTURE_TIMEOUT, log=None, capture_cls=None):
        if not issubclass(capture_cls, _AsyncCapture):
            raise ValueError(f'input value capture_cls should be instance of AsyncCapture.')

        self._capture_cls = capture_cls
        log = log or logging.getLogger(f'Async Tshark Capture <{name}')
        super(AsyncCaptureService, self).__init__(log=log, exe_timeout=exe_timeout)

    @check_event(EventID.running.value, True)
    async def get_packet(self, timeout=None):
        return await self._capture.get_packet(timeout)

    def _set_init_vars(self, **kwargs):
        self._live_capture_init_var = kwargs

    def update_init_vars(self, **kwargs):
        init_var = self._live_capture_init_var

        for name, value in kwargs.items():
            if name not in kwargs:
                raise Exception(f'Key {name} is not init value.')

            init_var[name] = value

    @check_event(EventID.finish.value, True)
    async def start(self, callback=None, packet_count=None, exe_timeout=None):
        exe_timeout = exe_timeout or self._exe_timeout
        self._reset()
        self._spwan_execute(self._live_capture_init_var ,callback, packet_count, exe_timeout)
        await self._wait_event(EventID.start.value)
        self._check_exception()
        await self._wait_event(EventID.running.value)
        self._check_exception()

    async def _prepare_live_capture(self, init_var) -> _AsyncLiveCapture:
        capture = self._capture_cls(**init_var)
        return capture

    async def _execute(self, init_var, callback, packet_count, exe_timeout):
        self._set_event(EventID.start.value)
        capture = None
        self._log.info('Async Live Capture Execute.')

        try:
            self._set_event(EventID.prepare.value)
            capture = await self._prepare_live_capture(init_var)
            self._set_event(EventID.running.value)
            self._log.info('Async Live Capture is running.')
            self._capture = capture
            await capture.capture_packet(packet_callback=callback, timeout=exe_timeout, packet_count=packet_count)
        except asyncTimeoutError:
            pass
        except CancelledError:
            pass
        except Exception as e:
            self._set_exception(e)
        finally:
            self._set_event(EventID.stop.value)

            if isinstance(capture, _AsyncLiveCapture):
                try:
                    await capture.close()
                except Exception as e:
                    logging.warning(f'Error occur when Live Capture is closing.\n{e}')
 
                del capture
 
            self._capture = None
 
        self._set_event(EventID.finish.value)
        self._log.info('Async Live Capture Finished.')

    def _reset(self):
        super()._reset()

class AsyncLiveCaptureService(AsyncCaptureService):
    def __init__(self, name='...', exe_timeout=LIVE_CAPTURE_DEFAULT_CAPTURE_TIMEOUT, interface=None,
                bpf_filter=None, display_filter=None, only_summaries=False, decryption_key=None,
                encryption_type='wpa-pwk', output_file=None, decode_as=None, disable_protocol=None,
                tshark_path=None, override_prefs=None, capture_filter=None, monitor_mode=False, use_json=False,
                 use_ek=False, include_raw=False, eventloop=None, custom_parameters=None, debug=False):

        log = logging.getLogger(f'Async Tshark Live Capture <{name}>')
        self._set_init_vars(interface=interface, bpf_filter=bpf_filter, display_filter=display_filter, only_summaries=only_summaries, decryption_key=decryption_key, encryption_type=encryption_type, output_file=output_file, decode_as=decode_as, disable_protocol=disable_protocol, tshark_path=tshark_path, override_prefs=override_prefs, capture_filter=capture_filter, monitor_mode=monitor_mode, use_json=use_json, use_ek=use_ek, include_raw=include_raw, eventloop=eventloop, custom_parameters=custom_parameters, debug=debug)
        super(AsyncLiveCaptureService, self).__init__(log=log, exe_timeout=exe_timeout\
            , name=name, capture_cls=_AsyncLiveCapture)