from async_util import print_loop_stack
from async_app_fw.lib.hub import TaskLoop, app_hub
from async_app_fw.base.app_manager import AppManager
import logging


async def application_init_and_run():
    app_hub.setup_eventloop()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        'async_app_fw.controller.mcp_controller.agent_handler',
        'custom_app.async_packet_capture_service.capture_service_agent_handler',
        'custom_app.api_action_app.api_action_agent_handler',
        'custom_app.async_command_executor_remote_app.command_executor_agent_handler'
    ])

    contexts = app_mgr.create_contexts()

    services = []
    services.extend(app_mgr.instantiate_apps(**contexts))
    # services.append(app_hub.spawn(print_loop_stack,
    # loop=app_hub.loop, print_task=True))

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
