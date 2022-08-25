from dataclasses import dataclass
import base64
import re
import logging
import traceback
import aiohttp
from async_app_fw.lib.hub import app_hub

def encrpt_password(pw):
    encrpt_pw = str(base64.b64encode(pw.encode()), 'utf-8')
    return encrpt_pw

class APINotLoginYet(Exception):
    pass

CONTROL_APP_NAME = None

GET = 'get'
POST = 'post'
PUT = 'put'
DELETE = 'delete'
PATHCH = 'patch'


def __re_check_token(text):
    is_match = re.search(r"token.is", text, flags=re.IGNORECASE)
    return not is_match

class RemoteResponse:
    def __init__(self, resp: aiohttp.ClientResponse):
        self.version = resp.version
        self.status = resp.status
        self.reason = resp.reason
        self.ok = resp.ok
        self.method = resp.method
        self.url = resp.url
        self.real_url = resp.real_url
        self.text = resp.text
        self.json = resp.json
        self.charset = resp.charset

@dataclass
class SessionInfo():
    api_hostname: str
    base_url: str
    login_info: dict
    auth: str = None

def check_token():
    def _decorator(func):
        async def _check_token(self, *args,  **kwargs):
            if self.request_session is None:
                raise APINotLoginYet()

            resp: aiohttp.ClientResponse = await func(self, *args, **kwargs)

            # token exist
            if __re_check_token(resp.text):
                return resp

            self.LOG.warning(
                f'API action <{self.host_ip}> not login yet.')
            raise APINotLoginYet()

        _check_token._method_name = func.__name__
        return _check_token
    return _decorator


class APIActionNotExist(Exception):
    pass


class CannotFindAuth(Exception):
    pass


SwitchAPIActionSave = {}


class APIAction:
    def __init__(self, base_url, login_info: dict, auth=None):
        self.request_session = aiohttp.ClientSession(
            loop=app_hub.loop)
        self.base_url = base_url
        # struct of login_info: {url:str, method:str, data:dict, auth_path:str}
        self.login_info = login_info
        self.auth = auth

        self.LOG = logging.getLogger(
            f'API Action , target url: <{self.base_url}>')

        self.method_set = {
            'get': self.get,
            'put': self.put,
            'post': self.post,
            'delete': self.delete,
            'patch': self.patch
        }

        if auth is not None:
            self.set_auth(auth)

    def get_session_info(self, api_hostname):
        info = SessionInfo(
            api_hostname,
            self.base_url,
            self.login_info,
            self.auth
        )

        return info


    def update_base_url(self, base_url):
        self.request_session._base_url = base_url

    async def login_api(self, login_info=None):
        self.LOG.info('Login API')

        login_info = login_info or self.login_info

        # release previouse client session.
        if isinstance(self.request_session, aiohttp.ClientSession):
            await self.request_session.close()
            del self.request_session

        # create a new session
        self.request_session = aiohttp.ClientSession(
            #connector=aiohttp.TCPConnector(ssl=False),
            loop=app_hub.loop
        )

        self.LOG.info(f'Login info \n{login_info}')

        data = login_info['data']
        url = login_info['url']

        # post login request
        try:
            res: aiohttp.ClientResponse = await self.method_set[login_info['method']](url, data)
        except Exception as e:
            logging.info(traceback.format_exc())
            raise e

        # take out auth
        auth_path = login_info['auth_path'].split('.')

        auth = res.json

        try:
            for key in auth_path:
                auth = auth[key]
        except Exception as e:
            raise CannotFindAuth(
                f"Can't find target Auth when login {self.base_url}.")

        # set auth
        self.set_auth(auth=auth)

        return auth

    async def _send_request(self, method, url, data=None, params=None, verify_ssl=False, without_base_url=False) -> aiohttp.ClientResponse:
        if without_base_url is False:
            url = self.base_url + url

        logging.info(f'method: {method}, request url {url}, data: {data}')


        async with self.request_session.request(method, url, json=data, params=params, verify_ssl=verify_ssl) as resp:
            resp.text = await resp.text()
            try:
                resp.json = await resp.json()
            except aiohttp.ContentTypeError as e:
                resp.json = None

        return resp

    @check_token()
    async def get(self, url, data=None, params=None, without_base_url=False, callback=None):
        resp = await self._send_request(GET, url, data=data, params=params, verify_ssl=False, without_base_url=without_base_url)

        if callback is not None:
            callback(resp=resp)

        return resp

    @check_token()
    async def post(self, url, data=None, without_base_url=False, callback=None):
        resp = await self._send_request(POST, url, data=data, verify_ssl=False, without_base_url=without_base_url)

        if callback is not None:
            callback(resp)

        return resp

    @check_token()
    async def put(self, url, data=None, without_base_url=False, callback=None):
        resp = await self._send_request(PUT, url,data=data, verify_ssl=False, without_base_url=without_base_url)

        if callback is not None:
            callback(resp)

        return resp

    @check_token()
    async def delete(self, url, data=None, without_base_url=False, callback=None):
        resp = await self._send_request(DELETE, url, data=data, verify_ssl=False, without_base_url=without_base_url)

        if callback is not None:
            callback(resp)

        return resp

    @check_token()
    async def patch(self, url, data=None, without_base_url=False, callback=None):
        resp = await self._send_request(PATHCH, url, data=data, verify_ssl=False, without_base_url=without_base_url)

        if callback is not None:
            callback(resp)

        return resp

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

    def set_auth(self, auth):
        self.auth = auth
        # apply to session's header
        self.request_session.headers['Authorization'] = f"Bearer {auth}"

    async def close(self):
        if isinstance(self.request_session, aiohttp.ClientSession):
            await self.request_session.close()
            del self.request_session