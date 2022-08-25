import logging
from time import time
import traceback
import asyncio
import pytest
import pytest_asyncio
from custom_app.api_action_app.api_action_master_handler import ReqAPILoginCheck, ReqGetAPIAction
from async_app_fw.base.app_manager import AppManager
from custom_app.job_app.job_util.job_class import JOB_RUNNING, REMOTE_MATER
from custom_app.job_app.job_util.job_subprocess import JobCommand
from custom_app.test_app.base_app import AsyncTestApp, Step
from custom_app.job_app.job_master_handler import RequestJobCreateV2
from custom_app.util.async_api_action import POST, APIAction
from custom_app.test_app.util import install_test_app

app_manager = AppManager.get_instance()

class APIActionTest(AsyncTestApp):
    async def prepare_test(self):
        self.api_action: APIAction = await ReqGetAPIAction.send_request('switch3')

    @Step('test', no_queue=True, ignore_exception=False)
    async def send_remote_request(self):
        api_action = self.api_action

        try:
            for _ in range(3):
                resp = await api_action.get('system/info', '169.254.0.1', timeout=2)
                # logging.info(resp.json)
                start = time()
                await asyncio.sleep(2)
                logging.info(f'time: {time() - start}')
        except Exception as e: 
            logging.info(traceback.format_exc())

        raise Exception('fuck')
        self.stop_test()

class TestRstp:
    async def test_case(self):
        logging.info('Start test')

        task = await install_test_app(
            APIActionTest, 'test count test', timeout=20)

        logging.info('Stop test')
