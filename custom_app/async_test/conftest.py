import asyncio
import logging
import traceback
import pytest
import pytest_asyncio
from async_app_fw.lib.hub import app_hub, TaskLoop
from custom_app.util.async_api_action import POST, APIAction, encrpt_password
from custom_app.api_action_app.constant import AGENT_LOCAL
from async_app_fw.base.app_manager import AppManager

spawn = app_hub.spawn

@pytest.fixture(scope="session", autouse=True)
def event_loop():
    yield app_hub.loop
    app_hub.loop.close()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def async_app_fw():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    
    # get application manager
    app_mgr = AppManager.get_instance()

    # prepare applications which should be loaded.
    app_list = ['async_app_fw.controller.mcp_controller.master_handler',
    'custom_app.job_app.job_master_handler',
    'custom_app.api_action_app.api_action_master_handler']

    # load application
    app_mgr.load_apps(app_list)

    logging.info('Start async application frameowrk.')

    contexts = app_mgr.create_contexts()
    app_mgr.instantiate_apps(**contexts)

    await asyncio.sleep(3)

    yield 

    logging.info('close async_app_fw.')

    await app_mgr.close()

@pytest_asyncio.fixture(scope="session", autouse=True)
async def api_actions(async_app_fw):
    api_actions = {}
    tasks = []
    pw = encrpt_password('qnap1234')

    async def login_api(api_action: APIAction, api_hostname, agent=AGENT_LOCAL):
        try:
            await api_action.login_api(agent=agent)
            api_actions[api_hostname] = api_action
            logging.info(f"APIAction <{api_hostname}> login from agent {agent} success.")
        except Exception as e:
            logging.warning(f"Can't login target api service {api_hostname} from agetn <{agent}>.\n Traceback: {traceback.format_exc()}")
            api_actions[api_hostname] = None

    for i in range(1,5):
        api_hostname = f'switch{i}'
        agent = f'169.254.0.{i}'
        base_url=f'https://10.88.0.1{i}/api/v1/' 
        login_info={
            'url': 'users/login',
            'method': POST,
            'data': {
                'username': 'admin',
                'password': pw
            },
            'auth_path': 'result'
        }
        api_action = APIAction(base_url, login_info=login_info)

        # create concurrent login task
        tasks.append(spawn(login_api, api_action, api_hostname, agent=agent))

    # wait all login task end.
    await TaskLoop(app_hub, tasks).wait_tasks()

    yield api_actions