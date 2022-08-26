import logging
from time import time
import traceback
import asyncio
from custom_app.api_action_app.api_action_master_handler import ReqGetAPIAction
from async_app_fw.base.app_manager import AppManager
from custom_app.test_app.base_app import AsyncTestApp, Step
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
                start = time()
                resp = await api_action.get('system/info', agent='169.254.0.1', timeout=2)
                logging.info(f'time: {time() - start}')
                logging.info(resp.json)
                await asyncio.sleep(2)
        except Exception as e: 
            logging.info(traceback.format_exc())

        self.stop_test()

class TestExample:
    async def test_case(self):
        logging.info('Start test')

        task = await install_test_app(
            APIActionTest, 'test count test', timeout=20)

        logging.info('Stop test')
