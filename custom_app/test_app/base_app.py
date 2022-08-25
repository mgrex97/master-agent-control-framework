import inspect
import logging
import asyncio
import traceback
from asyncio import CancelledError, Event, Queue, Task
from time import time

from async_app_fw.base.app_manager import BaseApp, AppManager
from async_app_fw.lib.hub import TaskLoop, app_hub

spawn = app_hub.spawn
app_manager: AppManager = AppManager.get_instance()


def _is_step_function(step_fun):
    if inspect.iscoroutinefunction(step_fun) and hasattr(step_fun, '_test_step'):
        return True
    else:
        return False


class Step():
    def __init__(self, name, while_mode=False, input_names=[], no_queue=False, auto_start=True, ignore_exception=True):
        self.name = name
        self.no_queue = no_queue
        self.input_name = set(input_names)
        self.step_task = []
        self.while_mode = while_mode
        self.auto_start = auto_start
        self.ignore_exception = ignore_exception

    def __call__(_self, fun):
        async def _step(self, input_name):
            # get queue by input_name
            if _self.no_queue is False:
                if (input_queue := _self._input_queue.get(input_name, None)) is None:
                    raise Exception(
                        f"Can't find target input_queue: <{input_name}")

            result = None

            while True:
                # not implement yet. wait multiple input queue.
                # get data from target input queue.
                if _self.no_queue is False:
                    try:
                        ((args, kwargs), input_time) = await input_queue.get()
                    except Exception as e:
                        logging.info(traceback.format_exc())
                try:
                    if _self.no_queue is False:
                        start_time = time()
                        # self.LOG.info(
                        # f'delay of input processing: {start_time - input_time} sec.')
                        result = await fun(self, *args, **kwargs)
                        # self.LOG.info(
                        # f'processing time: {time() - start_time} sec.')
                    else:
                        logging.info('no queue await')
                        await fun(self)
                except Exception as e:
                    if _self.ignore_exception is False:
                        self.result = e
                        self.exception_event.set()
                        break
                    else:
                        self.LOG.warning(
                            f'Exception happend when step <{_self.name}> get input from queue:\n{traceback.format_exc()}')

                if _self.while_mode is False:
                    break
                    
            return result

        _self.step_fun = _step

        # add attribute
        fun._test_step = _self
        fun.step_name = _self.name
        fun.test_async_fun = _step
        fun.push_input = _self.push_input
        fun.stop = _self.stop
        fun.start = _self.start

        return fun

    def stop(self) -> Task:
        async def _stop(self):
            for task in self.step_task:
                task.cancel()
            await TaskLoop(app_hub, self.step_task).wait_tasks()
            # need to implement pop feature.
        return spawn(_stop, self)

    def push_input(self, *args, **kwargs,):
        input_name = kwargs.pop('input_name', '_default_input')

        if input_name == '_default_input':
            self._input_queue[input_name].put_nowait(
                ((args, kwargs), time()))
        else:
            if input_name not in self.input_name:
                raise Exception(
                    f'target input name <{input_name}> is not existed at test step {self.name}.')
            self._input_queue[input_name].put_nowait((args, kwargs), time())

    def start(self, test_instance) -> Task:
        logging.info(f'Start test step <{self.name}>')
        # Can't create Queue at init function, the queue will attatch to different event loop.
        self._input_queue = {'_default_input': Queue()}

        task = spawn(self.step_fun, test_instance, '_default_input')
        self.step_task.append(task)

        return task


class AsyncTestApp(BaseApp):
    def __init__(self, test_name, timeout=10, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
        self.name = f'TestApp <{test_name}>'
        self.LOG = logging.getLogger(f'{self.name}:')
        self.test_name = test_name
        self.timeout = timeout
        self._timeout_detector_task = None
        self.step_dict = {}
        self.job_dict = {}
        self.result = None
        self.end_event = Event()
        self._end_event = Event()
        self.exception_event = Event()
        self._collect_step_info()

    def create_job(self):
        pass

    async def prepare_test(self):
        pass

    async def start_test(self):
        # check prepare test is corotines
        try:
            await self.prepare_test()
        except Exception as e:
            # stop test if prepare failed.
            self.stop_test()
            raise e

        self._start_step()

    def _start_step(self):
        for _, step_fun in self.step_dict.items():
            if step_fun._test_step.auto_start is True:
                step_fun.start(self)

    async def create_job(self, name, job):
        self.job_dict = {name: job}

    def _collect_step_info(self):
        # get all step method from self
        # filter method which has _test_step attribure.
        step_dict = self.step_dict
        for _, step_fun in inspect.getmembers(self, predicate=_is_step_function):
            if step_fun.step_name in step_dict:
                raise Exception(f'Duplicated step name {step_fun.step_name}.')

            step_dict[step_fun.step_name] = step_fun

    async def _timeout_detector(self):
        if self.timeout is not None and self.timeout > 0:
            try:
                await asyncio.sleep(self.timeout)
                self.stop_test(result=TimeoutError(
                    f'Test timeout <{self.timeout}> sec.'))
            except CancelledError as e:
                pass

    def start(self):
        task = super().start()
        self._timeout_detector_task = spawn(self._timeout_detector)
        self._exception_detector_task = spawn(self._exception_detector)

        return [task, self._exception_detector, spawn(self._end_detector), self._timeout_detector_task, spawn(self.start_test)]

    def stop_test(self, result=None):
        self.LOG.info(f'Stop Test {self.name}')

        self.result = result or self.result

        for step_name, step_fun in self.step_dict.items():
            self.LOG.info(f'Stop test step: {step_name}')
            step_fun.stop()

        self._end_event.set()

        self._timeout_detector_task.cancel()
        self._exception_detector_task.cancel()

    async def _exception_detector(self):
        try:
            await self.exception_event.wait()
            self.stop_test()
        except CancelledError as e:
            pass

    async def _end_detector(self):
        await self._end_event.wait()
        # do something
        self.end_event.set()
        app_manager.uninstantiate(self.name)

    async def wait_test_end(self):
        await self.end_event.wait()

        if isinstance(self.result, Exception):
            raise self.result

        return self.result

    async def _event_loop(self):
        try:
            while self.is_active or self.events.empty():
                ev, state = await self.events.get()
                handlers = self.get_handlers(ev, state)
                for handler in handlers:
                    try:
                        handler(ev)
                    except:
                        self.LOG.exception('%s: Exception occurred during handler processing. '
                                           'Backtrace from offending handler '
                                           '[%s] servicing event [%s] follows.',
                                           self.name, handler.__name__, ev.__class__.__name__)
        except asyncio.CancelledError as e:
            self.LOG.info(f'App {self.name}, _event_loop been canceled.')

        # clean events queue.
        try:
            while self.events.get_nowait():
                pass
        except asyncio.QueueEmpty:
            pass
