import base64
import re
from pathlib import Path

import requests
from async_app_fw.lib.hub import app_hub

from custom_app.job_app.job_util.job_request import APIAction, JobRequest


def check_token(func):
    def wrap(*args, **kwargs):
        res = func(*args, **kwargs)
        api_object = args[0]
        text = res.text
        is_match = re.search(r"token.is", text, flags=re.IGNORECASE)
        if is_match:
            api_object.login_api()
            res = func(*args, **kwargs)
        return res

    return wrap


class SwitchAPIAction:
    def __init__(self):
        try:
            self.hostname = 'qsw-m408'
            self.host_ip = '10.88.0.14'
            self.username = 'admin'
            self.password = 'qnap1234'
            self.api_version = 'v1'
            self.request_session = requests.session()
        except KeyError as err:
            raise KeyError(
                f"{self.__class__.__name__} config not contain key {err}.")

        self.login_path = 'users/login'
        self.logout_path = 'users/logout'
        self.reboot_path = 'system/reboot'

    def login_api(self):
        data = {
            'username': self.username,
            'password': self.encrpt_password()
        }
        if self.api_version == 'v3':
            data['rememberme'] = True
        res = self.post(self.login_path, data)
        res_json = res.json()
        auth = res_json['result']['AccessToken'] if self.api_version == 'v3' else res_json['result']
        self.set_request_auth(auth=auth)
        return res

    # logout api not implement yet.

    def get_login_info(self):
        data = {
            'username': self.username,
            'password': self.encrpt_password()
        }
        if self.api_version == 'v3':
            data['rememberme'] = True

        login_info = {
            'host_ip': self.host_ip,
            'method': 'post',
            'path': self.get_api_url_with_path(self.login_path),
            'data': data,
            'host_name': self.hostname
        }
        login_info['auth_path'] = "result.AccessToken" if self.api_version == 'v3' else 'result'

        return login_info

    def logout_api(self):
        res = self.post(self.logout_path)
        return res

    def reboot_device(self):
        res = self.post(self.reboot_path)
        return res

    def encrpt_password(self):
        pw = str(base64.b64encode(self.password.encode()), 'utf-8')
        return pw

    def get_api_url(self, path):
        url = f"https://{self.host_ip}/api/{self.api_version}/{path}"
        return url

    def get_api_url_with_path(self, path):
        return self.get_api_url(path)

    @check_token
    def get(self, path, params=None):
        url = self.get_api_url(path)
        res = self.request_session.get(url=url, params=params, verify=False)
        return res

    @check_token
    def post(self, path, data=None):
        url = self.get_api_url(path)
        r = self.request_session.post(url=url, json=data, verify=False)
        return r

    @check_token
    def put(self, path, data=None):
        url = self.get_api_url(path)
        res = self.request_session.put(url=url, json=data, verify=False)
        return res

    @check_token
    def delete(self, path, data=None):
        url = self.get_api_url(path)
        res = self.request_session.delete(url=url, json=data, verify=False)
        return res

    @check_token
    def patch(self, path, data=None):
        url = self.get_api_url(path)
        res = self.request_session.patch(url=url, json=data, verify=False)
        return res

    def post_file(self, path, file_name):
        url = self.get_api_url(path)
        file = Path(file_name)
        file_data = open(file_name, "rb")
        file_tuple = (file.name, file_data, 'application/octet-stream')
        files = {"file": file_tuple}
        r = self.request_session.post(url=url, files=files, verify=False)
        return r

    def set_request_auth(self, auth):
        self.request_session.headers['Authorization'] = f"Bearer {auth}"


if __name__ == '__main__':
    async def main(api_action: APIAction):
        res = await api_action.login_api()

    switch_api = SwitchAPIAction()
    login_info = switch_api.get_login_info()
    api_action = APIAction(login_info)
    task = app_hub.spawn(main, api_action)
    app_hub.joinall([task])
