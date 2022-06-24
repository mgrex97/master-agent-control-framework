import asyncio
import functools
import json
import logging
import re
from time import time
import traceback
from typing import Awaitable
import requests
from async_app_fw.lib.hub import app_hub
from custom_app.job_app.job_util.job_class import JOB_RUN, JOB_RUNNING, JOB_STOP, JOB_STOPING, REMOTE_MATER, Job, action_handler, handle_state_change, observe_output, collect_handler

REQUEST_JOB = 2

GET = 'get'
POST = 'post'
PUT = 'put'
DELETE = 'delete'
PATHCH = 'patch'


def __re_check_token(text):
    is_match = re.search(r"token.is", text, flags=re.IGNORECASE)
    return not is_match


LOG = logging.getLogger('API Action')


# atleast retry one
def check_token(retry_login=1):
    def _decorator(func):
        async def _check_token(self, *args, **kwargs):
            if self.request_session is None:
                await self.login_api()

            for retry in range(retry_login):
                res = await func(self, *args, **kwargs)

                # token exist
                if __re_check_token(res.text):
                    return res

                LOG.warning(
                    f'API action <{self.host_ip}> not login yet, try login: {retry + 1}')
                await self.login_api()

        _check_token._method_name = func.__name__
        return _check_token
    return _decorator


class APIActionNotExist(Exception):
    pass


SwitchAPIActionSave = {}


class APIAction:
    def __init__(self, login_info: dict):
        self.request_session = None
        self.host_ip = login_info['host_ip']
        self.login_info = login_info
        self.LOG = logging.getLogger(
            f'API Action , target ip: <{self.host_ip}>  ')

        self.method_set = {
            'get': self.get,
            'put': self.put,
            'post': self.post,
            'delete': self.delete,
            'patch': self.patch
        }

    async def login_api(self, login_info=None):
        self.LOG.info('Login API')
        if login_info is None:
            login_info = self.login_info
        else:
            self.login_info = login_info

        if self.request_session is not None:
            del self.request_session
        self.request_session = requests.session()

        self.LOG.info(f'Login info \n{login_info}')

        data = login_info['data']
        url = login_info['path']
        # take out auth
        res = await self.method_set[login_info['method']](url, data)

        auth_path = login_info['auth_path'].split('.')
        auth = res.json()

        for key in auth_path:
            auth = auth[key]

        # set auth
        self.set_request_auth(auth=auth)
        return res

    def __send_request(self, method, *args, **kwargs) -> Awaitable:
        return app_hub.loop.run_in_executor(
            None, functools.partial(method, *args, **kwargs))

    @check_token()
    async def get(self, url, params=None):
        res = await self.__send_request(self.request_session.get, url=url, params=params, verify=False)
        return res

    @check_token()
    async def post(self, url, data=None):
        res = await self.__send_request(
            self.request_session.post, url=url, json=data, verify=False)
        return res

    @check_token()
    async def put(self, url, data=None):
        res = await self.__send_request(self.request_session.put, url=url, json=data, verify=False)
        return res

    @check_token()
    async def delete(self, url, data=None):
        res = await self.__send_request(self.request_session.delete, url=url, json=data, verify=False)
        return res

    @check_token()
    async def patch(self, url, data=None):
        res = await self.__send_request(self.request_session.patch, url=url, json=data, verify=False)
        return res

    """
    def post_file(self, path, file_name):
        url = self.get_api_url(path)
        file = Path(file_name)
        file_data = open(file_name, "rb")
        file_tuple = (file.name, file_data, 'application/octet-stream')
        files = {"file": file_tuple}
        r = self.request_session.post(url=url, files=files, verify=False)
        return r
    """

    def set_request_auth(self, auth):
        self.request_session.headers['Authorization'] = f"Bearer {auth}"

    @classmethod
    def get_APIAction(cls, host_ip, config: dict):
        if (api_action := SwitchAPIActionSave.get(host_ip, None)) is None:
            api_action = SwitchAPIActionSave[host_ip] = cls(config)

        return api_action


@collect_handler
@Job.register_job_type(REQUEST_JOB)
class JobRequest(Job):
    def __init__(self, request_info: dict, connection=None, timeout=60, state_inform_interval=5, remote_mode=False, remote_role=None):
        super().__init__(connection, timeout, state_inform_interval,
                         remote_mode=remote_mode, remote_role=remote_role)
        self.request_info = request_info
        self.times = request_info.get('running_times', 5)
        self.retry_mode = request_info.get('retry_mode', False)
        self.output_method = None

        if self.retry_mode is True:
            self.retry_data = request_info['retry_data']
            self.retry_interval = request_info.get('retry_interval', 1)
        else:
            self.request_queue = asyncio.Queue()

        if remote_mode is True and remote_role == REMOTE_MATER:
            return

        self.api_action: APIAction = APIAction.get_APIAction(
            self.request_info['host_ip'], self.request_info['login_info'])

    @classmethod
    def create_job_by_job_info(cls, job_info, connection, remote_role=None):
        return cls(job_info['request_info'], connection,
                   job_info['timeout'], job_info['state_inform_interval'], job_info['remote_mode'], remote_role)

    def set_output_method(self, method):
        self.output_method = method

    def job_info_serialize(self):
        output = super().job_info_serialize()
        output['request_info'] = self.request_info
        return output

    @action_handler(JOB_RUN, JOB_RUNNING)
    def run(self):
        pass

    async def spawn_request_with_callback(self, type, url, data):
        res: requests.Response = await self.api_action.method_set[type](url, data)
        try:
            self.request_handler(res.json())
        except json.decoder.JSONDecodeError:
            self.LOG.warning("Can't decode response.")

    @handle_state_change((JOB_RUN, JOB_RUNNING), JOB_STOP)
    async def request_consumer(self):
        time_tmp = time() + self.times
        try:
            if self.retry_mode is True:
                type = self.retry_data['type']
                url = self.retry_data['url']
                data = self.retry_data.get('data', None)
            while time() < time_tmp:
                if self.retry_mode is False:
                    (type, url, data) = await self.request_queue.get()
                    app_hub.spawn(
                        self.spawn_request_with_callback, type, url, data)
                else:
                    app_hub.spawn(
                        self.spawn_request_with_callback, type, url, data)
                    await asyncio.sleep(self.retry_interval)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            exception_info = traceback.format_exc(e)
        # cancel all task

    def push_request_data_to_queue(self, type, url, data):
        assert self.retry_mode is False
        self.request_queue.put_nowait((type, url, data))

    @observe_output(JOB_RUNNING)
    def request_handler(self, state, result):
        if self.output_method is not None:
            method = self.output_method
            method(self.request_info, result)
        else:
            print(result)

    @action_handler(JOB_STOP, JOB_STOPING, cancel_current_task=True)
    async def stop(self):
        pass

    def __del__(self):
        pass
