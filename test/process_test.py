import logging
import json
import asyncio
from eventlet_framework.lib import hub
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


class ProcessNotRunningYet(Exception):
    pass


@Job.register_job_type(CMD_JOB)
class JobCommand(Job):
    def __init__(self, command, connection=None, timeout=60, state_inform_interval=5):
        super().__init__(connection, timeout, state_inform_interval)
        self.command = command
        self.__process = None
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.std_task = {}

    @classmethod
    def create_job_by_job_info(cls, job_info, connection):
        return cls(job_info['cmd_job_info']['command'], connection,
                   job_info['timeout'], job_info['state_inform_interval'])

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def job_info_serialize(self):
        output = super().job_info_serialize()
        output['cmd_job_info'] = {
            'command': self.command
        }
        return output

    def run_job(self):
        super().run_job()
        return self.run_command()

    def run_command(self):
        async def _run_command():
            print(f'run {self.command}')
            self.__process = await asyncio.create_subprocess_shell(
                self.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            self.stdin = self.__process.stdin
            self.stdout = self.__process.stdout
            self.stderr = self.__process.stderr
            self.state = True
            self.read_stdout()
            task_loop = hub.TaskLoop(
                hub.app_hub, list(self.std_task.values()))
            await task_loop.wait_tasks()

        return hub.app_hub.spawn(_run_command)

    def read_stdout(self):
        self.read_output_from_std(self.stdout, 'out')

    def read_stderr(self):
        self.read_output_from_std(self.stderr, 'err')

    """
    def output_handler(self, output_info):
        # prevent queue from full.
        if self.output_queue.full():
            self.output_queue.get()

        self.output_queue.put(output_info)
        LOG.warning(
            f'Get CMD output from {self.connection.address}, Output: {output_info}')
    """

    def read_output_from_std(self, std, type):
        if std is None:
            raise ProcessNotRunningYet()

        async def __read_output_from_std(std: asyncio.StreamReader):
            while self.state and self.__process:
                get = await std.readline()
                # get, err = await self.__process.communicate()
                print(get)

                output_info = super(JobCommand, self).job_info_serialize()
                output_info['output'] = get.decode(encoding='utf-8')

        # if task not exist yet.
        if (task := self.std_task.get(type, None)) is None:
            task = hub.app_hub.spawn(__read_output_from_std, std)

        # if stdout is done, create new task.
        if task.done():
            del task
            task = hub.app_hub.spawn(__read_output_from_std, std)

        self.std_task.setdefault(type, task)

    def stop(self):
        super().stop()

        async def _subprocess_stop():
            # make sure all std tasks are canceled.
            tasks = self.std_task.values()
            for task in tasks:
                if not task.done():
                    task.cancel()

            task_loop = hub.TaskLoop(hub.app_hub.loop, tasks)
            await task_loop.wait_tasks()

        self.stdin = None
        self.stderr = None
        self.stdout = None

        return hub.app_hub.spawn(_subprocess_stop)

    def __del__(self):
        self.stop()


job = JobCommand('ping 8.8.8.8')
task = job.run_job()

hub.app_hub.joinall([task])

"""
   async def test_job():
        self.install_job(JobCommand(
            'ping 8.8.8.8'), '127.0.0.1')
        self.install_job(JobCommand(
            'ping 168.95.1.1'), '127.0.0.1')
        self.install_job(JobCommand(
            'ping 192.168.100.1'), '127.0.0.1')
        await asyncio.sleep(3)
        self.clear_job('127.0.0.1')

app_hub.spawn(test_job)
"""
