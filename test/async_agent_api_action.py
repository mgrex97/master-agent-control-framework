from async_app_fw.lib.hub import TaskLoop, app_hub
from async_app_fw.base.app_manager import AppManager
import logging


async def application_init_and_run():
    app_hub.setup_eventloop()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        # 'custom_app.job_app.job_agent_handler',
        'custom_app.api_action_app.api_action_agent_handler'
    ])

    contexts = app_mgr.create_contexts()

    services = []
    services.extend(app_mgr.instantiate_apps(**contexts))

    task_loop = TaskLoop(app_hub, services)
    try:
        await task_loop.wait_tasks()
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        await app_mgr.close()

task = app_hub.spawn(application_init_and_run)
app_hub.joinall([task])
