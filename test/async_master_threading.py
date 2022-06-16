import time
import asyncio
import logging

from eventlet_framework.base.app_manager import AppManager, SERVICE_BRICKS
from eventlet_framework.lib.hub import Hub, TaskLoop, app_hub
from async_util import print_loop_stack

from concurrent.futures import ThreadPoolExecutor


async def application_init_and_run(app_mgr: AppManager):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr.load_apps([
        # 'eventlet_framework.controller.mcp_controller.master_handler',
        'custom_app.job_app.job_master_handler'
    ])

    contexts = app_mgr.create_contexts()

    services = []
    services.extend(app_mgr.instantiate_apps(**contexts))
    # services.append(app_hub.spawn(
    # print_loop_stack, loop=app_hub.loop, print_task=True))

    task_loop = TaskLoop(app_hub, services)
    try:
        await task_loop.wait_tasks()
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        pass
        await app_mgr.close()


def app_thread_entrance(app_mgr):
    app_hub.setup_eventloop()
    task = app_hub.spawn(application_init_and_run, app_mgr)
    app_hub.joinall([task])
    app_hub.joinall()


async def test():
    for i in range(10):
        print(10)
        await asyncio.sleep(1)

if __name__ == '__main__':
    app_mgr = AppManager.get_instance()

    hub = Hub()

    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(app_thread_entrance, app_mgr)
        hub.setup_eventloop()
        task = hub.spawn(test)
        hub.joinall([task])
