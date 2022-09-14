import asyncio
import traceback
from async_app_fw.base.app_manager import AppManager
import logging
from async_app_fw.lib.hub import app_hub
from custom_app.util.async_command_executor import AsyncCommandExecutor

spawn = app_hub.spawn

async def run_cmd():
    await asyncio.sleep(5)

    cmd = AsyncCommandExecutor(command='ping 8.8.8.8', name='ping test', exe_timeout=10)

    print('start ping cmd')

    await cmd.start(exe_timeout=10, agent='127.0.0.1')
    task = spawn(wait_cmd_stop, cmd)

    while await cmd.is_running():
        try:
            get = await cmd.read_stdout()
        except Exception as e:
            print(traceback.format_exc())
            break

        print(get, end='')

    await task
    
async def wait_cmd_stop(cmd: AsyncCommandExecutor):
    await cmd.wait_finished(timeout=20)
    cmd._check_exception()
    print('Tshark stop.')

async def application_init_and_run():
    app_hub.setup_eventloop()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        'async_app_fw.controller.mcp_controller.master_handler',
        'custom_app.async_command_executor_remote_app.command_executor_master_handler'
    ])

    contexts = app_mgr.create_contexts()

    app_mgr.instantiate_apps(**contexts)
    services = []
    services.append(spawn(run_cmd))
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