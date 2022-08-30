import logging
from time import time
import traceback
import asyncio
from async_app_fw.base.app_manager import AppManager
from custom_app.test_app.base_app import AsyncTestApp, Step
from custom_app.util.async_api_action import APIAction
from custom_app.test_app.util import install_test_app

app_manager = AppManager.get_instance()

class APIActionTest(AsyncTestApp):
    def __init__(self, test_name, api_actions, timeout=10, *_args, **_kwargs):
        super().__init__(test_name, timeout, *_args, **_kwargs)
        self.api_actions = api_actions

    async def prepare_test(self):
        self.api_action:APIAction = self.api_actions['switch1']

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
    async def test_case(self, api_actions):
        logging.info('Start test')

        task = await install_test_app(
            APIActionTest, 'test count test', api_actions, timeout=20)

        logging.info('Stop test')
