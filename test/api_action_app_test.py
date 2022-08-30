import base64
import asyncio
import traceback
import aiohttp
import logging
from async_app_fw.base.app_manager import AppManager
from async_app_fw.lib.hub import TaskLoop, app_hub
from async_util import print_loop_stack
from custom_app.util.async_api_action import POST, APIAction, encrpt_password

async def api_action_test():
    logging.basicConfig(level=logging.INFO)
    await asyncio.sleep(5)
    base_url='https://10.88.0.11/api/v1/'
    login_info={
        'url': 'users/login',
        'method': POST,
        'data': {
            'username': 'admin',
            'password': encrpt_password('qnap1234'),
        },
        'auth_path': 'result'
    }

    api_action: APIAction = APIAction(base_url=base_url, login_info=login_info)

    try:
        await api_action.login_api(agent='169.254.0.1')


        url = f'https://10.88.0.11/api/about'
        while True:
            resp: aiohttp.ClientResponse = await api_action.get(url, agent='169.254.0.2', without_base_url=True)
            data = resp.json
            print(f'Get {data} from {api_action.base_url}')
            # print(f'{api_action.request_session.headers}')
    except Exception as e:
        logging.info(traceback.format_exc())

async def application_init_and_run():
    logging.basicConfig(level=logging.INFO)

    app_hub.setup_eventloop()

    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        'custom_app.api_action_app.api_action_master_handler'
    ])

    contexts = app_mgr.create_contexts()

    services = []
    app_mgr.instantiate_apps(**contexts)
    services.append(app_hub.spawn(api_action_test))
    # services.append(app_hub.spawn(print_loop_stack,
    # loop=app_hub.loop, print_task=True))

    task_loop = TaskLoop(app_hub, services)
    try:
        await task_loop.wait_tasks()
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        await app_mgr.close()

if __name__ == '__main__':
    task = app_hub.spawn(application_init_and_run)
    app_hub.joinall([task])
