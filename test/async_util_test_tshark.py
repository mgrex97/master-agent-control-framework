from async_app_fw.lib.hub import app_hub
from custom_app.util.async_tshark import AsyncLiveCaptureService
from custom_app.util.async_utility import EventStateCheckFalied
from custom_app.util.async_pyshark_lib.capture.async_capture import AsyncCaptureStop

spawn = app_hub.spawn

if __name__ == '__main__':
    async def run_capture():
        capture = AsyncLiveCaptureService(name='tshark test', exe_timeout=15, interface='en6', bpf_filter='icmp')

        await capture.start(exe_timeout=10)
        task = spawn(wait_capture_stop, capture)

        while await capture.is_running():
            try:
                get = await capture.get_packet()
                print(get)
            except AsyncCaptureStop:
                break
 
        await task
        
    async def wait_capture_stop(capture: AsyncLiveCaptureService):
        await capture.wait_finished(timeout=20)
        print('Tshark stop.')
 

    task = app_hub.spawn(run_capture)
    app_hub.joinall([task])