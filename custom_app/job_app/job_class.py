import logging
import json
import asyncio
import traceback
from async_app_fw.lib import hub
from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from pprint import pprint

# job state
JOB_CREATE = 1
JOB_ASYNC = 2
JOB_RUNING = 3
JOB_DELETE = 4
JOB_CREATE_FAILD = 5

# job type
CMD_JOB = 1

LOG = logging.getLogger('custom_app.job_app.jog_class')


class Job:
    _Job_Types = {}

    def __init__(self, connection=None, timeout=60, state_inform_interval=5):
        self.id = 0
        self.state = JOB_CREATE
        self.connection = connection
        self.timeout = timeout
        self.state_inform_interval = state_inform_interval

    @classmethod
    def create_job_by_job_info(cls, connection, job_info, job_id):
        assert job_id > 0

        if isinstance(job_info, str):
            job_info = json.loads(job_info)

        job_type = job_info['job_type']
        job_obj = cls._Job_Types[job_type].create_job_by_job_info(
            job_info, connection)
        job_obj.set_job_id(job_id)
        return job_obj

    @staticmethod
    def register_job_type(job_type):
        def _register_job_type(cls):
            assert job_type not in Job._Job_Types
            cls.cls_job_type = job_type
            Job._Job_Types[job_type] = cls
            return cls
        return _register_job_type

    def set_job_id(self, job_id):
        self.id = job_id

    def change_state(self, state):
        self.state = state

    def parse(self, payload):
        pass

    def output_handler(self, output_info):
        pass

    def run_job(self):
        self.change_state(JOB_RUNING)

    def job_info_serialize(self, output=None):
        if output is None:
            output = dict()

        output = {
            'job_type': self.cls_job_type,
            'job_id': self.id,
            'job_state': self.state,
            'timeout': self.timeout,
            'state_inform_interval': self.state_inform_interval
        }

        return output

    def stop(self):
        pass
