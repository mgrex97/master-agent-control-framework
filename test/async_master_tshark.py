import asyncio
from async_app_fw.base.app_manager import AppManager
import logging
from async_app_fw.lib.hub import app_hub
from custom_app.util.async_tshark import AsyncLiveCaptureService
from custom_app.util.async_pyshark_lib.capture.async_capture import AsyncCaptureStop

spawn = app_hub.spawn

async def run_capture():
    await asyncio.sleep(5)
    capture = AsyncLiveCaptureService(name='tshark test', exe_timeout=15, interface='en6', bpf_filter='icmp')

    print('start capture')

    await capture.start(exe_timeout=10, agent='127.0.0.1')
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
    capture._check_exception()
    print('Tshark stop.')

async def application_init_and_run():
    app_hub.setup_eventloop()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        'async_app_fw.controller.mcp_controller.master_handler',
        'custom_app.async_packet_capture_service.capture_service_master_handler'
    ])

    contexts = app_mgr.create_contexts()

    app_mgr.instantiate_apps(**contexts)
    services = []
    services.append(spawn(run_capture))
    # services.append(app_hub.spawn(print_loop_stack,
    # loop=app_hub.loop, print_task=True))

    try:
        await asyncio.wait(services)
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        pass
        await app_mgr.close()

if __name__ == '__main__':
    task = app_hub.spawn(application_init_and_run)
    app_hub.joinall([task])