import logging
import json
import asyncio
import traceback
from eventlet_framework.lib import hub
from eventlet_framework.controller.mcp_controller.mcp_controller import MachineConnection
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


class ProcessStillRunning(Exception):
    pass


@Job.register_job_type(CMD_JOB)
class JobCommand(Job):
    def __init__(self, command, connection=None, timeout=60, state_inform_interval=5):
        super().__init__(connection, timeout, state_inform_interval)
        self.command = command
        self.command_running_task = None
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
        self.run_command()

    def run_command(self, auto_stop=False):
        async def _run_command():
            try:
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
                # wait std read task end
                task_loop = hub.TaskLoop(
                    hub.app_hub, list(self.std_task.values()))
                await task_loop.wait_tasks()
            except asyncio.CancelledError:
                pass
            finally:
                self.stop()
                self.command_running_task = None

        if self.command_running_task is None:
            pass
        elif not self.command_running_task.done():
            if auto_stop is True:
                self.command_running_task.cancel()
            else:
                raise ProcessStillRunning()

        self.command_running_task = hub.app_hub.spawn(_run_command)
        return self.command_running_task

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
            try:
                while self.state and self.__process:
                    # get = await std.readline()
                    get = await std.readline()
                    print(get)
                    if len(get) == 0:
                        break
                    output_info = super(JobCommand, self).job_info_serialize()
                    output_info['output'] = get.decode(encoding='utf-8')

                    output_msg = self.connection.mcproto_parser.MCPJobOutput(
                        self.connection, self.id, json.dumps(output_info))

                    self.connection.send_msg(output_msg)
            except asyncio.CancelledError:
                pass

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

        async def _subprocess_stop(process):
            # make sure all std tasks are canceled.
            if isinstance(process, asyncio.subprocess.Process):
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                    # LOG.warning(
                    #    f'Process Kill Exception, {traceback.format_exc()}')
                except Exception:
                    LOG.warning(
                        f'Process Kill Exception, {traceback.format_exc()}')

            std_type = list(self.std_task.keys())
            for k in std_type:
                std_task = self.std_task.pop(k)
                if not std_task.done():
                    std_task.cancel()

        process = self.__process
        self.__process = None
        self.stdin = None
        self.stderr = None
        self.stdout = None
        self.connection = None

        # if there is std task exist.
        if self.std_task:
            return hub.app_hub.spawn(_subprocess_stop, process)
        else:
            None

    def __del__(self):
        if self.command_running_task:
            self.command_running_task.cancel()
