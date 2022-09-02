import logging
from time import time
import traceback
import asyncio
from async_app_fw.base.app_manager import AppManager
from custom_app.test_app.base_app import AsyncTestApp, Step
from custom_app.util.async_api_action import APIAction
from custom_app.util.async_command_executor import AsyncCommandExecutor
from other_resource import other_path

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
    async def test_api_action_remote_feature(self, api_actions):
        api_action:APIAction = api_actions['switch1']
        logging.info('Start test')

        try:
            for _ in range(3):
                start = time()
                resp = await api_action.get('system/info', agent='169.254.0.1', timeout=2)
                logging.info(f'time: {time() - start}')
                logging.info(resp.json)
                await asyncio.sleep(2)
        except Exception as e:
            logging.info(traceback.format_exc())

        logging.info('Stop test')

    async def test_async_command(self):
        async_command = AsyncCommandExecutor(command='ping -c 3 169.254.0.1', timeout=15)
        await async_command.start()

        while async_command.is_running():
            get = await async_command.read_stdout()
            logging.info(get)

    async def test_wrong_command(self):
        async_command = AsyncCommandExecutor(command='ping -c 3 ', timeout=15)
        await async_command.start()

        while async_command.is_running():
            get = await async_command.read_stdout()
            logging.info(get)
 
        async_command.result()

    async def test_interaction(self):
        async_command = AsyncCommandExecutor(command=f'sh {other_path}/interaction_test.sh', timeout=15)
        await async_command.start()

        get = await async_command.read_stdout()

        logging.info(get)

        assert 'Enter Your Name:' in get

        await async_command.write_stdin('carltonlin\n')

        get = await async_command.read_stdout(timeout=6)

        logging.info(get)

        assert "Welcome carltonlin!" in get