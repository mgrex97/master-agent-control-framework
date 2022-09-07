from asyncio import wait_for, CancelledError, TimeoutError as asyncTimeoutError\
    , StreamReader, StreamWriter, create_subprocess_shell, subprocess
import logging
from custom_app.util.async_utility import AsyncUtility, check_event
from async_app_fw.lib.hub import app_hub
from .constant import AsyncUtilityEventID as EventID

spawn = app_hub.spawn

class CommandNotSetup(Exception):
    pass

class CommandIsExecuting(Exception):
    pass

class CommandNotExecuteYet(Exception):
    pass

class CommandStop(Exception):
    def __init__(self, *args: object, command='') -> None:
        self.command = command
        super().__init__(*args)

class CommandCrash(Exception):
    pass

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

class AsyncCommandExecutor(AsyncUtility):
    def __init__(self, command ,name='...', exe_timeout=DEFAULT_STOP_TIMEOUT):
        log = logging.getLogger(f'Async Command Executor <{name}>')
        super().__init__(log=log, exe_timeout=exe_timeout)
        self.command = command
        self._eof_reached = False

    def _reset(self):
        super()._reset()
        self._stdin = self._stdout = self._stderr = None

    @check_event(EventID.finish.value, True)
    async def start(self, command=None, exe_timeout=None):
        command = command or self.command
        exe_timeout = exe_timeout or self._exe_timeout
        self._reset()
        self._spwan_execute(command, exe_timeout)

        # wait event been set. sequence: start->prepare->running
        for event_id in (EventID.start.value, \
                EventID.prepare.value, EventID.running.value):

            await self._wait_event(event_id)
            self._check_exception()

    @check_event(EventID.stop.value, False)
    async def read_stdout(self, timeout=READ_TIMEOUT, read_char=None, read_until=None):
        try:
            get = await wait_for(self._read_std(self._stdout, read_char=read_char, read_until=read_until), timeout)
        except asyncTimeoutError:
            raise StdOutReadTimeout()

        return get

    @check_event(EventID.stop.value, False)
    async def read_stderr(self, timeout=READ_TIMEOUT, read_char=None, read_until=None):
        try:
            get = await wait_for(self._read_std(self._stderr, read_char=read_char, read_until=read_until), timeout)
        except asyncTimeoutError:
            raise StdOutReadTimeout()

        return get

    @check_event(EventID.stop.value, False)
    async def write_stdin(self, data: str, timeout=READ_TIMEOUT):
        self._stdin.write(data.encode('utf-8'))

        try:
            await wait_for(self._stdin.drain(), timeout)
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
        process = await create_subprocess_shell(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        self._stdin = process.stdin
        self._stdout = process.stdout
        self._stderr = process.stderr

        if process.returncode is not None and process.returncode != 0:
            raise CommandCrash(f"{command} seems to have crashed.")

        return process

    async def _execute(self, command, exe_timeout):
        process = None        

        self._log.info(f'Command Execute.')
        try:
            self._set_event(EventID.start.value)
            process = await self._prepare_subprocess(command)
            self._set_event(EventID.prepare.value)
            self._log.info(f'Command {self.command} is running now.')
            self._set_event(EventID.running.value)
            await wait_for(process.wait(), exe_timeout)
        except asyncTimeoutError:
            pass
        except CancelledError:
            # ignore CancelledError
            self._log.info(f'Command {self.command} execute timeout.')
        except CommandCrash as e:
            self._set_exception(e)
        except Exception as e:
            self._set_exception(e)
        finally:
            self._set_event(EventID.stop.value)
            if isinstance(process, subprocess.Process):
                try:
                    await self._cleanup_subprocess(process, command)
                except Exception as e:
                    self._log.warning('cleanup subprocess falied.')

                try:
                    await self._cleanup_std(command)
                except Exception as e:
                    self._log.warning('cleanup StreamWriter and StreamReader falied.')

        self._set_event(EventID.finish.value)
        self._log.info(f'Command {self.command} running end.')

    async def _cleanup_subprocess(self, process: subprocess.Process, command):
        """Kill the given process and properly closes any pipes connected to it."""
        if process.returncode is None:
            try:
                process.kill()
                return await wait_for(process.wait(), 1)
            except TimeoutError:
                self._log.debug("Waiting for process to close failed, may have zombie process.")
            except ProcessLookupError:
                pass
        elif process.returncode > 0:
            if process.returncode != 1 or self._eof_reached:
                raise CommandCrash(f"{command} seems to have crashed. (retcode: %d")

    async def _cleanup_std(self, command):
        """Clean up StreamWriter and StreamReader create by process."""
        for std in (self._stdin, self._stdout, self._stderr):
            if isinstance(std, StreamWriter):
                std.close()
                await std.wait_closed()
            elif isinstance(std, StreamReader):
                try:
                    std.set_exception(CommandStop(f'Command <{command}> Stop.'))
                except Exception:
                    pass
