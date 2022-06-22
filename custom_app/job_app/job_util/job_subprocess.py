import logging
import json
import asyncio
import traceback

from async_app_fw.lib import hub
from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from custom_app.job_app.job_util.job_class import Job, JOB_RUN, JOB_RUNNING, JOB_STOP, JOB_STOPED, JOB_STOPING, action_handler, collect_handler, handle_state_change, observe_output

CMD_JOB = 1


class ProcessNotRunningYet(Exception):
    pass


class ProcessStillRunning(Exception):
    pass


LOG = logging.getLogger('Job Process Command')


@collect_handler
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

    @observe_output(JOB_RUNNING)
    def get_output(state, output):
        print(output)

    @action_handler(JOB_RUN, JOB_RUNNING)
    def run(self):
        pass

    @handle_state_change((JOB_RUN, JOB_RUNNING), JOB_STOP)
    async def _run_command(self):
        try:
            self.__process = await asyncio.create_subprocess_shell(
                self.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            self.stdin = self.__process.stdin
            self.stdout = self.__process.stdout
            self.stderr = self.__process.stderr
            self.read_stdout()
            # wait std read task end
            task_loop = hub.TaskLoop(
                hub.app_hub, list(self.std_task.values()))
            await task_loop.wait_tasks()
        except asyncio.CancelledError:
            pass

        self.LOG.info(f'Command {self.command} running end.')

    def read_stdout(self):
        self.read_output_from_std(self.stdout, 'out')

    def read_stderr(self):
        self.read_output_from_std(self.stderr, 'err')

    def read_output_from_std(self, std, type):
        if std is None:
            raise ProcessNotRunningYet()

        async def __read_output_from_std(std: asyncio.StreamReader):
            try:
                while self.state and self.__process:
                    # get = await std.readline()
                    get = await std.readline()
                    if len(get) == 0:
                        break
                    output_info = super(JobCommand, self).job_info_serialize()
                    output_info['output'] = get.decode(encoding='utf-8')

                    self.get_output(get)
                    # output_msg = self.connection.mcproto_parser.MCPJobOutput(
                    # self.connection, self.id, json.dumps(output_info))
                    # self.connection.send_msg(output_msg)
            except asyncio.CancelledError:
                pass
            except Exception:
                print(traceback.format_exc())

        # if task not exist yet.
        if (task := self.std_task.get(type, None)) is None:
            task = hub.app_hub.spawn(__read_output_from_std, std)

        # if stdout is done, create new task.
        if task.done():
            del task
            task = hub.app_hub.spawn(__read_output_from_std, std)

        self.std_task.setdefault(type, task)

    @action_handler(JOB_STOP, JOB_STOPING)
    def stop(self):
        self.stdin = None
        self.stderr = None
        self.stdout = None

    @handle_state_change((JOB_STOP, JOB_STOPING), JOB_STOPED)
    async def _subprocess_stop(self):
        process = self.__process
        self.__process = None
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

    def __del__(self):
        self.stop()
