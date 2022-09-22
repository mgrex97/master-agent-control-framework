from typing import Dict
from abc import ABC, abstractmethod
import logging
import asyncio
from asyncio import Event

from async_app_fw.lib.hub import app_hub
from .constant import AsyncUtilityEventID, EVENT_NAMES_MAPPING

spawn = app_hub.spawn

DEFALUT_ASYNC_UTILITY_EXE_TIMEOUT = 60 # 60 sec
CHECK_EVENT_DEFAULT_TIMEOUT = 0

__default_event_id__ = list(map(lambda x: x.value, AsyncUtilityEventID._member_map_.values()))

class EventNotExist(Exception):
    pass

class EventStateCheckFalied(Exception):
    pass

class UtilityStillRunning(Exception):
    pass

class UtilityIsNotRunning(Exception):
    pass

def get_default_event_id():
    return __default_event_id__.copy()

IS_SET = True
IS_NOT_SET = False

def check_event(event_id, set_or_not: bool, timeout=0):
    def _decorator(func):
        async def _check_event(self, *args, **kwargs):
            event_name = EVENT_NAMES_MAPPING.get(event_id, None) or event_id
            # check event exist.
            if (event := self._events.get(event_id, None)) is None:
                raise EventNotExist(f"Can't find target event {event_name}.")

            # check event sate is same as set_or_not
            if event.is_set() is not set_or_not:
                # wait event to be set.
                if set_or_not is True and timeout != 0:
                    await asyncio.wait_for(event.wait(), timeout)

                raise EventStateCheckFalied(f'Expect event <{event_name}> is_set should be {set_or_not}.')
 
            return await func(self, *args, **kwargs)
        return _check_event
    return _decorator

class AsyncUtility(ABC):
    def __init__(self, log=None, events_id=None, exe_timeout=DEFALUT_ASYNC_UTILITY_EXE_TIMEOUT) -> None:
        events_id = events_id or __default_event_id__

        self._exe_timeout = exe_timeout
        self._log = log or logging.getLogger(f'Async Utility')
        self._events: Dict[int, Event] = {}
        self._exception = None
        self._execute_task = None
        self._init_event(events_id)

    @abstractmethod
    def _reset(self):
        self._exception = None
        for event in self._events.values():
            event.clear()

    def _init_event(self, events_id):
        for event in events_id:
            event_instance = Event()
            event_instance.set()
            self._events[event] = event_instance

    @abstractmethod
    async def _execute(self, timeout=DEFALUT_ASYNC_UTILITY_EXE_TIMEOUT):
        pass

    def _spwan_execute(self, *args ,**kwargs):
        self._execute_task = spawn(self._execute, *args, **kwargs)

    def _cancel_execute(self):
        if not isinstance(self._execute_task, asyncio.Task) or self._execute_task.done() is True:
            raise UtilityIsNotRunning()
        
        self._execute_task.cancel()

    def _set_event(self, event_id):
        self._events[event_id].set()

    async def _wait_event(self, event_id, timeout=0):
        event = self._events[event_id]

        if timeout == 0:
            await event.wait()
        else:
            await asyncio.wait_for(event.wait(), timeout)

    def _check_event_is_set(self, event_id):
        return self._events[event_id].is_set()

    def _set_exception(self, exception):
        if not isinstance(exception, Exception):
            raise Exception(f'Input value exception should be instance of Exception.')
 
        self._exception = exception

    def _check_exception(self):
        if isinstance(self._exception, Exception):
            raise self._exception

    @abstractmethod
    async def start(self):
        pass

    @check_event(AsyncUtilityEventID.stop.value, False)
    async def stop(self):
        self._cancel_execute()
        await self._wait_event(AsyncUtilityEventID.stop.value)
        self._check_exception()

    async def wait_finished(self, timeout=0):
        try:
            await self._wait_event(AsyncUtilityEventID.finish.value, timeout)
        except asyncio.TimeoutError:
            raise UtilityStillRunning()

    async def is_running(self):
        return self._check_event_is_set(AsyncUtilityEventID.running.value) and \
            not self._check_event_is_set(AsyncUtilityEventID.stop.value)

    def is_running_no_await(self):
        return self._check_event_is_set(AsyncUtilityEventID.running.value) and \
            not self._check_event_is_set(AsyncUtilityEventID.stop.value)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, value, tarceback):
        if self.is_running_no_await():
            return
        self._log.info(f'Wait Service stop.')
        try:
            await self.stop()
        except Exception as e:
            pass
        self._log.info(f'Service stopped.')