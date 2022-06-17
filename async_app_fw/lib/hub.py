import asyncio
import logging
import os
import inspect
import socket
import threading
from async_app_fw.lib import ip
from signal import SIGINT
from async_app_fw.utils import _listify


class SpawnFailed(Exception):
    pass


class ServerNotInitYet(Exception):
    pass


class ServerInitFailed(Exception):
    pass


Queue = asyncio.Queue

# logging.basicConfig(level=logging.WARNING)


class TaskLoop(object):
    def __init__(self, hub, tasks, handler=None, timeout=None):
        self.hub = hub
        self.tasks = _listify(tasks)
        self._init_iter()
        self.handler = handler
        self.timeout = timeout
        self.LOG = logging.getLogger('Task Loop: ')
        self.LOG.setLevel(logging.WARNING)

    def print_task_info(self, task: asyncio.Task):
        pass

    def reset_tasks(self, tasks):
        self.tasks = _listify(tasks)
        self._init_iter()

    def _init_iter(self):
        self.tasks_iter = iter(self.tasks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            task: asyncio.Task = next(self.tasks_iter)
        except StopIteration:
            raise StopAsyncIteration

        # self.LOG.info(f'*** wait task <{task.get_name()}> running... ***')
        await task
        # self.LOG.info(f'*** task <{task.get_name()}> running end... ***')

        if self.handler is None:
            return task

        self.print_task_info(task=task)

        result = task.result()

        if inspect.isasyncgenfunction(task):
            await self.handler(task.result())
        else:
            self.handler(result)

        return task

    def wait_tasks(self):
        async def _wait_tasks():
            async for _ in self:
                pass

        return self.hub.spawn(_wait_tasks)


class Hub():
    LOG = logging.getLogger('async_hub')
    LOG.setLevel(logging.WARNING)

    def __init__(self):
        self.setup_eventloop()
        self.loop.add_signal_handler(
            SIGINT, Hub.sigtstp_handler, SIGINT, self.loop)

    def _signal_handler(self):
        pass

    @classmethod
    def sigtstp_handler(cls, sig, loop):
        def keyboradInterrupt():
            raise KeyboardInterrupt

        for task in asyncio.all_tasks(loop=loop):
            task.cancel()

        cls.LOG.info(f'Got signal: {sig!s}, shutting down.')

    def setup_eventloop(self):
        if os.name == "nt":
            self.eventloop = asyncio.ProactorEventLoop()
        else:
            try:
                self.loop = asyncio.get_event_loop()
            except RuntimeError:
                if threading.current_thread() != threading.main_thread():
                    # Ran not in main thread, make a new eventloop
                    self.loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self.loop)
                else:
                    raise
        if os.name == "posix" and isinstance(threading.current_thread(), threading._MainThread):
            asyncio.get_child_watcher().attach_loop(self.loop)

    def spawn(self, func, * args, **kwargs):
        async def _spawn(func, *args, **kwargs):
            name = func.__name__

            try:
                self.LOG.info(f'Spawn {name}')

                if inspect.iscoroutinefunction(func):
                    self.LOG.info(f'Spawn Task {name}')
                    await func(*args, **kwargs)
                elif inspect.isfunction(func) or inspect.ismethod(func):
                    self.LOG.info('Spawn callback')
                    self.loop.call_soon(func, *args)
                else:
                    raise SpawnFailed(f"Can't Spawn <{name}> failed.")
            except StopIteration as e:
                self.LOG.warning('Stop Iteration.')
            except asyncio.CancelledError as e:
                self.LOG.warning(f'Spawn task <{name}> get CancelledError.')
            finally:
                self.LOG.info(f'Spawn end: {name}')

        task = self.loop.create_task(_spawn(func, *args, **kwargs))
        task.set_name(f'Task Spawn: <{func.__name__}>')
        return task

    def kill(self, task: asyncio.Task):
        task.cancel()

    def joinall(self, tasks):
        async def _joinall(tasks):
            self.LOG.info('Await Joinall')
            while True:
                try:
                    await asyncio.gather(*tasks)
                    self.LOG.info('Joinall Finished.')
                    break
                except asyncio.CancelledError:
                    self.LOG.info('Joinall get cancel error. Keep running...')

        self.loop.run_until_complete(_joinall(tasks))


app_hub = Hub()


class StreamServer(object):
    def __init__(self, listen_info, handle=None, backlog=None,
                 spawn='default', **ssl_args):

        assert ip.valid_ipv4(listen_info[0]) or ip.valid_ipv6(listen_info[0])

        self.LOG = logging.getLogger(
            f'Stream Server, Listen on : {listen_info} ----')
        self.listen_info = listen_info
        self.handle = handle
        self.server = None

    async def _init_server(self):
        try:
            self.server: asyncio.base_events.Server = await asyncio.start_server(
                self.handle, *self.listen_info)
        except socket.gaierror:
            self.LOG.warning("Socket's ip or port number is wrong.")
            raise ServerInitFailed()

    async def serve_forever(self):
        if self.server is None:
            await self._init_server()

        async with self.server:
            self.LOG.info('Stream Sever start to serve.')
            await self.server.wait_closed()
            self.LOG.info('Stream Sever serve end.')


class StreamClient(object):
    def __init__(self, addr, timeout=None, **ssl_args):
        assert ip.valid_ipv4(addr[0]) or ip.valid_ipv6(addr[0])

        self.LOG = logging.getLogger(
            f'Stream Client, Connect to: {addr} ----')
        self.addr = addr
        self.timeout = timeout
        self.ssl_args = ssl_args
        self._is_active = True

    async def connect(self):
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(*self.addr), timeout=self.timeout)
        except socket.error as e:
            self.LOG.warning(f'Connection Faield. {e}')
            return None

        return (reader, writer)

    def connect_loop(self, handle, interval=0) -> asyncio.Task:
        async def _connect_loop(handle, interval):
            while self._is_active:
                result = await self.connect()
                if result:
                    await app_hub.spawn(handle, result[0], result[1])

                await asyncio.sleep(interval)
                self.LOG.info(f'Connect again after {interval} sec.')
            self.LOG.info(f'connect_loop stop. Get CancelledError.')

        return app_hub.spawn(_connect_loop, handle, interval)

    def stop(self):
        self._is_active = False
