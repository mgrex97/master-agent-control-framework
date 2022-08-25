import asyncio
import logging
import signal
import pytest
from concurrent.futures import ThreadPoolExecutor
from async_app_fw.lib.hub import app_hub, TaskLoop
from app_init_code.init_master import init_basic_app
from custom_app.api_action_app.api_action_master_handler import ReqAPILoginCheck
from custom_app.util.async_api_action import POST, SessionInfo, encrpt_password

@pytest.fixture(scope="session", autouse=True)
def async_app_fw():
    logging.info('Start async application frameowrk.')

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(
        init_basic_app,
        ['async_app_fw.controller.mcp_controller.master_handler',
         'custom_app.job_app.job_master_handler',
         'custom_app.api_action_app.api_action_master_handler']
    )

    yield future

    with executor:
        if future.done() is not True:
            # raise SIGINT to stop.
            signal.raise_signal(signal.SIGINT)

    logging.info('leave from async_app fixture')

@pytest.fixture(scope="session", autouse=True)
def async_init_api_action():
    # read config from ini file
    # ini api action from

    async def init_api_action():
        tasks = []
        pw = encrpt_password('qnap1234')

        for i in range(1,5):
            api_hostname = f'switch{i}'
            session_info = SessionInfo(
                api_hostname=api_hostname,
                base_url=f'https://10.88.0.1{i}/api/v1/', 
                login_info={
                    'url': 'users/login',
                    'method': POST,
                    'data': {
                        'username': 'admin',
                        'password': pw
                    },
                    'auth_path': 'result'
                }
            )
            tasks.append(ReqAPILoginCheck.send_request(api_hostname, session_info=session_info))
        
        await TaskLoop(app_hub, tasks).wait_tasks()

    future = app_hub.spawn(init_api_action)
    future.result()