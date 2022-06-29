import logging
import json
import asyncio
import traceback

from async_app_fw.lib import hub
from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from custom_app.job_app.job_util.job_class import JOB_DELETE, JOB_DELETED, JOB_RUN, JOB_RUNNING, JOB_STOP, JOB_STOPED, JOB_STOPING, REMOTE_MATER
from custom_app.job_app.job_util.job_class import Job, collect_handler
from custom_app.job_app.job_util.job_class import ObserveOutput, HandleStateChange, ActionHandler

CMD_JOB = 1


class ProcessNotRunningYet(Exception):
    pass


class ProcessStillRunning(Exception):
    pass


LOG = logging.getLogger('Job Process Command')


@collect_handler
@Job.register_job_type(CMD_JOB)
class JobCommand(Job):
    def __init__(self, command, connection=None, timeout=60, state_inform_interval=5, remote_mode=False, remote_role=None):
        super().__init__(connection, timeout, state_inform_interval,
                         remote_mode=remote_mode, remote_role=remote_role)
        self.command = command
        self.__process = None
        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.std_task = {}

    @classmethod
    def create_job_by_job_info(cls, job_info, connection, remote_role=None):
        return cls(job_info['cmd_job_info']['command'], connection,
                   job_info['timeout'], job_info['state_inform_interval'], job_info['remote_mode'], remote_role)

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def job_info_serialize(self):
        output = super().job_info_serialize()
        output['cmd_job_info'] = {
            'command': self.command
        }
        return output

    @ObserveOutput(JOB_RUNNING)
    def get_output(self, state, output):
        print(output)

    @ActionHandler(JOB_RUN, JOB_RUNNING)
    def run(self):
        pass

    @HandleStateChange((JOB_RUN, JOB_RUNNING), JOB_STOP)
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
                    get = await std.readline()
                    if len(get) == 0:
                        break
                    output = get.decode(encoding='utf-8')

                    self.get_output(output)
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

    @ActionHandler(JOB_STOP, JOB_STOPING, cancel_current_task=True)
    def stop(self):
        for std in (self.stdin, self.stderr, self.stdout):
            if isinstance(std, asyncio.StreamWriter):
                std.close()
                del std
            elif isinstance(std, asyncio.StreamReader):
                del std

        self.stdin = None
        self.stderr = None
        self.stdout = None

    @HandleStateChange((JOB_STOP, JOB_STOPING), JOB_STOPED)
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

    @ActionHandler(JOB_DELETE, JOB_DELETED, require_before=True, cancel_current_task=True)
    async def delete(self, before=None):
        # append stop into handler_exe_queue
        if before != JOB_STOPED:
            self.stop(cancel_current_task=False)
            self._subprocess_stop()
            # append delete into handler_exe_queue
            self.delete(cancel_current_task=False)
        else:
            # wait output queue and exe handler queue stop.
            await super().delete()

    def __del__(self):
        self.delete()
