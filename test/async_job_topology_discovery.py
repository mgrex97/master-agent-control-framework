import asyncio
import logging

from async_app_fw.base.app_manager import AppManager, lookup_service_brick
from async_app_fw.lib.hub import TaskLoop, app_hub
from custom_app.job_app.job_util.job_class import REMOTE_MATER


async def application_init_and_run():
    app_hub.setup_eventloop()

    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        # 'async_app_fw.controller.mcp_controller.master_handler',
        # 'custom_app.job_app.job_master_handler'
        'custom_app.topology_discovery_app.topology_discovery'
    ])

    contexts = app_mgr.create_contexts()

    services = []
    services.extend(app_mgr.instantiate_apps(**contexts))
    # job_app = lookup_service_brick('job_master_handler')
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

if __name__ == '__main__':
    task = app_hub.spawn(application_init_and_run)
    app_hub.joinall([task])
