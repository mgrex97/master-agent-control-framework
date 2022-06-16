import asyncio
import logging
from eventlet_framework.base.app_manager import AppManager, SERVICE_BRICKS
from eventlet_framework.lib.hub import TaskLoop, app_hub
from async_util import print_loop_stack


async def application_init_and_run():
    app_hub.setup_eventloop()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
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

task = app_hub.spawn(application_init_and_run)
app_hub.joinall([task])
