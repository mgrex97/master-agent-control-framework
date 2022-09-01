from asyncio import subprocess
import inspect
import logging
import asyncio

from async_app_fw.lib.hub import app_hub

spawn = app_hub.spawn

asyncTimeoutError = asyncio.exceptions.TimeoutError

class CommandNotSetup(Exception):
    pass

class CommandIsExecuting(Exception):
    pass

class CommandNotExecuteYet(Exception):
    pass

class CommandCrash(Exception):
    def __init__(self, *args: object, process=None) -> None:
        super().__init__(*args)
        self.process= process

class StdErrorReadTimeout(Exception):
    pass

class StdOutReadTimeout(Exception):
    pass

class StdinWriteTimeout(Exception):
    pass

COMMAND_EXECUTE_TIMEOUT = 30

DEFAULT_STOP_TIMEOUT = 5

READ_TIMEOUT = 10
WRITE_TIMEOUT = 10

NOT_RUNNING = False
IS_RUNNING = True

def __running_check(current_state, target_state):
    if current_state is not target_state:
        if target_state is True:
            raise CommandNotExecuteYet()
        else:
            raise CommandIsExecuting()

def check_running(running):
    def decorater(method):
        if inspect.iscoroutinefunction(method):
            async def _check_running(self, *args, **kwargs):
                __running_check(self._is_running, running)
                return await method(self, *args, **kwargs)
            return _check_running
        else:
            def _check_running(self, *args, **kwargs):
                __running_check(self._is_running, running)
                return method(self, *args, **kwargs)
            return _check_running
    return decorater

class AsyncCommandExecutor:
    def __init__(self, command=None, timeout=COMMAND_EXECUTE_TIMEOUT, name='') -> None:
        self.name = f'Async Command Executor {name}'
        self._log = logging.getLogger(self.name)
        self.command = command
        self.timeout = timeout
        self._is_running = False
        self._stop_event = asyncio.Event()
        self._command_start_event = asyncio.Event()
        self._stop_event.set()
        self._command_start_event.set()

    def _reset(self):
        self._execute_task = None
        self._execute_exception = None
        self._stop_event.clear()
        self._command_start_event.clear()

        self._stdin = None
        self._stdout = None
        self._stderr = None
        self._eof_reached = False

    @check_running(NOT_RUNNING)
    async def start(self, command=None):
        command = command or self.command

        if command is None:
            raise CommandNotSetup()

        self._reset()
        self._execute_task = spawn(self._execute, command)
        await self._wait_command_start(self.timeout)

        return self._execute_task

    def is_running(self):
        if self._execute_exception is not None:
            raise self._execute_exception

        return not self._stop_event.is_set()

    async def wait_until_stop(self, timeout=0):
        try:
            await asyncio.wait_for(self._stop_event.set(), timeout)
        except asyncTimeoutError:
            raise CommandIsExecuting()

        if self._execute_exception is not None and isinstance(self._execute_exception, Exception):
            raise self._execute_exception

    @check_running(IS_RUNNING)
    async def stop(self, timeout=DEFAULT_STOP_TIMEOUT):
        self._execute_task.cancel()
        await self.wait_until_stop(timeout)

    @check_running(IS_RUNNING)
    async def read_stderr(self, read_char=None, timeout=WRITE_TIMEOUT, read_until=None):
        try:
            get = await asyncio.wait_for(self._read_std(self._stderr, read_char=read_char, read_until=read_until), timeout)
        except asyncTimeoutError:
            raise StdErrorReadTimeout()

        return get

    @check_running(IS_RUNNING)
    async def read_stdout(self, timeout=READ_TIMEOUT, read_char=None, read_until=None):
        try:
            get = await asyncio.wait_for(self._read_std(self._stdout, read_char=read_char, read_until=read_until), timeout)
        except asyncTimeoutError:
            raise StdOutReadTimeout()

        return get

    @check_running(IS_RUNNING)
    async def write_stdin(self, data: str, timeout=READ_TIMEOUT):
        self._stdin.write(data.encode('utf-8'))

        try:
            await asyncio.wait_for(self._stdin.drain(), timeout)
        except asyncTimeoutError:
            raise StdinWriteTimeout()

    async def _read_std(self, std, read_char=None, read_until=None):
        if read_char is not None and isinstance(read_char, int):
            get = await std.readexactly(read_char)
        elif read_until is not None:
            get = await std.readuntil(read_until.encode('utf-8'))
        else:
            get = await std.readline()
 
        return get.decode(encoding='utf-8')

    async def _prepare_subprocess(self, command):
        process = await asyncio.create_subprocess_shell(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        self.process = process
        self._stdin = process.stdin
        self._stdout = process.stdout
        self._stderr = process.stderr

        if process.returncode is not None and process.returncode != 0:
            raise CommandCrash(f"{command} seems to have crashed.)", process=process)

        return process

    async def _execute(self, command):
        process = None
        ignore_process_cleanup_exception = False

        try:
            process = await self._prepare_subprocess(command)
            self._log.info(f'Command {self.command} is running now.')
            self._command_start_event.set()
            self._is_running = True
            await asyncio.wait_for(process.wait(), self.timeout)
        except asyncTimeoutError:
            pass
        except asyncio.CancelledError:
            # ignore CancelledError
            pass
        except CommandCrash as e:
            process = e.process
            self._execute_exception = e
            ignore_process_cleanup_exception = True
        except Exception as e:
            self._execute_exception = e
            ignore_process_cleanup_exception = True
        finally:
            self._is_running = False
            if isinstance(process, subprocess.Process):
                try:
                    await self._cleanup_subprocess(process, command)
                except Exception as e:
                    # just ignore the 
                    if ignore_process_cleanup_exception is not True:
                        self._execute_exception = e

                await self._cleanup_std()

            self._stop_event.set()

        self._log.info(f'Command {self.command} running end.')

    async def _cleanup_subprocess(self, process: subprocess.Process, command):
        """Kill the given process and properly closes any pipes connected to it."""
        if process.returncode is None:
            try:
                process.kill()
                return await asyncio.wait_for(process.wait(), 1)
            except asyncio.exceptions.TimeoutError:
                self._log.debug("Waiting for process to close failed, may have zombie process.")
            except ProcessLookupError:
                pass
        elif process.returncode > 0:
            if process.returncode != 1 or self._eof_reached:
                raise CommandCrash(f"{command} seems to have crashed. (retcode: %d")

    async def _cleanup_std(self):
        """Clean up StreamWriter and StreamReader create by process."""
        for std in (self._stdin, self._stdout, self._stderr):
            if isinstance(std, asyncio.StreamWriter):
                std.close()
                await std.wait_closed()
                del std
            elif isinstance(std, asyncio.StreamReader):
                del std

    async def _wait_command_start(self, timeout):
        await asyncio.wait_for(self._command_start_event.wait(), timeout)