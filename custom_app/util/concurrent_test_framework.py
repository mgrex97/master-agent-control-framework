import inspect
import logging
import asyncio
import traceback
import itertools
from asyncio import Task
from typing import Dict, List

from async_app_fw.lib.hub import app_hub

spawn = app_hub.spawn

def _is_step_function(step_fun):
    if inspect.iscoroutinefunction(step_fun) and hasattr(step_fun, '_test_step'):
        return True
    else:
        return False


class Step():
    def __init__(self, name=None, auto_start=True, loop_mode=False, ignore_exception=[], dont_save_exception=[], save_exception=True):
        self.name = name
        self.input_queue = asyncio.Queue()
        self.save_exception = save_exception
        self._loop_mode = loop_mode
        self.step_task = None
        self.auto_start = auto_start
        self.ignore_exception = set(ignore_exception)
        self.dont_save_exception = set(dont_save_exception)

    def __call__(_self, fun):
        _self.name = _self.name or fun.__name__
    
        async def _step(self: ConcurrentTest):
            # get attribute from _self.
            save_exception = _self.save_exception
            dont_save_exception = _self.dont_save_exception
            ignore_exception = _self.ignore_exception
            self.LOG.info(f'Start step <{_self.name}>')

            # infinit loop or loop which only runs one times.
            tmp = itertools.count(start=1) if _self._loop_mode == True else range(1)

            for _ in tmp:
                try:
                    await fun(self)
                except Exception as e:
                    e_class = e.__class__
                    if save_exception and e_class not in dont_save_exception:
                        # save exception
                        traceback_str = traceback.format_exc()
                        self._add_exception(e, traceback_str)

                    if e_class in ignore_exception:
                        self.LOG.warning(
                            f'Exception Ignore. Step-<{_self.name}>: \n{traceback.format_exc()}')
                        continue
                    else:
                        # stop test
                        self.stop_test() 
                        break

            self.LOG.info(f'End step <{_self.name}>')

        _self.step_fun = _step

        # add attribute to funciotn
        fun._test_step = _self
        fun.step_name = _self.name
        fun.test_async_fun = _step
        fun.start = _self.start

        return fun

    def start(self, test_instance) -> Task:
        return spawn(self.step_fun, test_instance)


class ConcurrentTest():
    def __init__(self, test_name='...'):
        self.name = f'Concurrent Test <{test_name}>'
        self.LOG = logging.getLogger(f'{self.name}')
        self.test_name = test_name
        self._tasks: Dict[str, asyncio.Task] = {}
        self._is_running = False
        self._step_dict: Dict[str, Step] = {}
        self._exception_list = []
        self._collect_step_info()

    async def prepare_test(self):
        pass

    def start_step(self, step_name):
        # if task not exist
        if not (task := self._tasks.get(step_name, None)):
            pass
        # if task exist.
        elif not (task.done() or task.cancelled()):
            raise Exception(f'Step name <{step_name}> is still running.')

        self._tasks[step_name] = self._step_dict['step_name'].start(self)

    def _start_default_steps(self):
        tasks = self._tasks
        for step_fun in self._step_dict.values():
            if step_fun._test_step.auto_start is True:
                step_name = step_fun.step_name
                if step_name in tasks:
                    raise Exception(f'Duplicate step name <{step_name}>.')

                tasks[step_name] = step_fun.start(self)

        return tasks

    def _collect_step_info(self):
        # get all step method from self
        # filter method which has _test_step attribure.
        step_dict = self._step_dict
        for _, step_fun in inspect.getmembers(self, predicate=_is_step_function):
            if step_fun.step_name in step_dict:
                raise Exception(f'Duplicated step name {step_fun.step_name}.')

            step_dict[step_fun.step_name] = step_fun

    async def start_test(self, timeout=None):
        test_timeout = None

        await self.prepare_test()
        self._start_default_steps()
        waitting = None

        try:
            self._is_running = True
            # wait test timeout. Notice: asycio.wait won't raise asyncio.TimeoutError. 
            (_, waitting) = await asyncio.wait(self._tasks.values(), timeout=timeout)
            # If there sitll remeaning tasks in watting list, raise timeout exception.
            if len(waitting) > 0:
                test_timeout = asyncio.TimeoutError(f'{self.name} timeout.')
        finally:
            if waitting is not None:
                # make sure all tasks are done.
                for task in waitting:
                    if task.cancelled():
                        continue
                    task.cancel()
                await asyncio.gather(*waitting)

        self._is_running = False
 
        if test_timeout:
            raise test_timeout

        if len(self._exception_list) > 0:
            raise Exception(self._exception_list)

    def stop_test(self):
        self.LOG.info(f'Stop Test {self.name}')

        if self._is_running is False:
            raise Exception(f'{self.name} is still running.')

        for task in self._tasks.values():
            if task.done() or task.cancelled():
                continue
            task.cancel()

    def _add_exception(self, exception, traceback_str):
        if not isinstance(exception, Exception):
            raise(f'Input variable {exception} should be instance of Exception.')
 
        self._exception_list.append(exception)

    def get_exception(self):
        if self._is_running is True:
            raise Exception(f'Concurrent Test is running.')
 
        return self._exception_list
