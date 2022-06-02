from eventlet_framework.lib import hub
import asyncio
import json

# job state
JOB_CREATE = 1
JOB_ASYNC = 2
JOB_RUNING = 3
JOB_DELETE = 4

# job type
CMD_JOB = 1


class Job:
    _Job_Types = {}

    def __init__(self, connection, timeout=60, state_inform_interval=5):
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
            connection, job_info)
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


@Job.register_job_type(CMD_JOB)
class JobCommand(Job):
    def __init__(self, connection, command, timeout=60, state_inform_interval=5):
        super().__init__(connection, timeout, state_inform_interval)
        self.command = command
        self.proc = None
        self.stdin = None
        self.stdout = None
        self.stderr = None

    @classmethod
    def create_job_by_job_info(cls, connection, job_info):
        return cls(connection, job_info['command'],
                   job_info['timeout'], job_info['state_inform_interval'])

    def run_job(self):
        super().run_job()
        self.__create_process(self.command)
        hub.spawn(self.read_stdout)

    def job_info_serialize(self):
        output = super().job_serialize()
        output['cmd_job_info'] = {
            'command': self.command
        }
        return json.dumps(output)

    async def __create_process(self, command):
        self.proc = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        self.stdin = self.proc.stdin
        self.stdout = self.proc.stdout
        self.stderr = self.proc.stderr

    async def read_stdout(self):
        assert self.state is not None

        while self.state is JOB_RUNING:
            get = await self.stdout.readline()
