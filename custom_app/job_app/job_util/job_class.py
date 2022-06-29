import inspect
import logging
import json
import asyncio
import traceback
from typing import Tuple

from async_app_fw.lib import hub
from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from async_app_fw.protocol.mcp.mcp_parser_v_1_0 import MCPJobOutput, MCPJobStateChange
from async_app_fw.utils import _listify

# job state, max size 16 (0~15)
JOB_ANY_STATE = 0

# action
JOB_CREATE = 1
JOB_RUN = 3
JOB_STOP = 6
JOB_DELETE = 10

JOB_INIT = 0

JOB_CREATED = 2

JOB_RUNNING = 4
JOB_FAIELD = 5

JOB_STOPING = 7
JOB_STOPED = 8

JOB_FINISHED = 9

JOB_DELETEING = 11
JOB_DELETED = 12

JOB_ANY_EXECEPT_SELF = 13
JOB_OUTPUT = 14

STATE_MAPPING = {
    1: "JOB_CREATE",
    3: "JOB_RUN",
    6: "JOB_STOP",
    10: "JOB_DELETE",

    0: "JOB_INIT",

    2: "JOB_CREATED",

    4: "JOB_RUNNING",
    5: "JOB_FAIELD",

    7: "JOB_STOPING",
    8: "JOB_STOPED",

    9: "JOB_FINISHED",

    11: "JOB_DELETEING",
    12: "JOB_DELETED",

    13: "JOB_ANY_EXECEPT_SELF",
    14: "JOB_OUTPUT"
}

TAKE_ACTION = (JOB_CREATE, JOB_RUN, JOB_STOP, JOB_DELETE)
ACTION_RESULT = (JOB_CREATED, JOB_RUNNING, JOB_FAIELD,
                 JOB_FINISHED, JOB_DELETEING, JOB_DELETED,
                 JOB_STOPING, JOB_STOPED, JOB_OUTPUT)


LOG = logging.getLogger('custom_app.job_app.jog_class')

REMOTE_MATER = 'master'
REMOTE_AGENT = 'agent'


class StageChangeAssignFail(Exception):
    pass


class StageChangeAfterIsAny(Exception):
    pass


class StageRemoteControlError(Exception):
    pass


class StatgeChangeAfterIsList(Exception):
    pass


log_collect_handler = logging.getLogger('collect handler')


def _state_change_to_key(before, after):
    for b in _listify(before):
        # give these exception method name.
        if after == JOB_ANY_STATE:
            raise StageChangeAfterIsAny()
        if isinstance(after, list):
            raise StatgeChangeAfterIsList()
        if b == after:
            raise StageChangeAssignFail()

        key = f'{b}-{after}'
    return key


def collect_handler(cls):
    cls._handler_set = {}
    cls._action_set = {}
    cls._observe_set = {}
    cls._observe_name_set = {}

    def _is_handler_or_action(handler):
        if inspect.isfunction(handler) and \
                (hasattr(handler, '_state_change') or
                 hasattr(handler, '_action') or
                 hasattr(handler, '_observe')):
            return True
        else:
            return False

    for _, handler in inspect.getmembers(cls, predicate=_is_handler_or_action):
        if hasattr(handler, '_state_change'):
            for state_change in handler._state_change.keys():
                handler_list = cls._handler_set.get(state_change, None)

                if handler_list is not None:
                    log_collect_handler.warning(
                        f'There has duplicated hanlders handle same state change. {state_change}')
                else:
                    handler_list = []
                    cls._handler_set.setdefault(state_change, handler_list)

                handler_list.append(handler)
        elif hasattr(handler, '_action'):
            cls._action_set[handler._action] = handler
        elif hasattr(handler, '_observe'):
            if handler._observer_name in cls._observe_name_set:
                raise Exception(
                    f"There already exist a observer with name '{handler._observer_name}'.")

            cls._observe_name_set[handler._observer_name] = handler

            for state in handler._observe:
                observe_list = cls._observe_set.get(state, None)

                if observe_list is not None:
                    log_collect_handler.warning(
                        f'There has duplicated hanlders handle same state change. {state}')
                else:
                    observe_list = []
                    cls._observe_set.setdefault(state, observe_list)

                observe_list.append(handler)

    return cls


class ObserveOutput(object):
    def __init__(self, state, agent_handle=False, remote_output=True) -> None:
        self.state = _listify(state)
        self.agent_handler = agent_handle
        self.remote_output = remote_output

    # deorate observer
    def __call__(_self, fun):
        _self.fun = fun

        # if function had already decorated.
        if hasattr(fun, '_observe'):
            # add state to _observe
            _self.set_action_dict(_self.state, fun._observe)
            # No need to create function _output_handler again.
            return

        # init observe_action, create _output_handler
        observe_action = {}
        _self.set_action_dict(_self.state, observe_action)

        # Detect remote_mode, send output to remote master or push to jobs's _output_queue.
        def _output_handler(self: Job, *args, state=None, **kwargs):
            current_state = state if state is not None else self.state

            async def output(*args, **kwargs):
                if inspect.iscoroutinefunction(fun):
                    await fun(self, current_state, *args, **kwargs)
                else:
                    fun(self, current_state, *args, **kwargs)

            if self.state not in observe_action:
                self.LOG.warning(
                    f"The current job state {STATE_MAPPING[self.state]} is not handler {fun.__name__} want.")
                return

            observe_obj: ObserveOutput = observe_action[self.state]

            # Assume at remote mode, job always running on agent,
            # then job need to output job to remote master.
            while True:
                if self.remote_mode is False:
                    break

                if self.remote_role is REMOTE_MATER:
                    break

                # remote agent need to return output to master if remote_output is True.
                if observe_obj.remote_output is True:
                    ObserveOutput._remote_output(
                        self, fun, *args, **kwargs)

                # If agent_handle is False there is no need to deal with output.
                if observe_obj.agent_handler is True:
                    break

                return

            # Put variable and output_observer (method) into output queue.
            ObserveOutput._put_output_handler(
                self, output, *args, **kwargs)

        # basic setting of _output_handler
        # Notice: observe_action -> list reference
        _output_handler._observe = observe_action
        _output_handler._observer_name = fun.__name__

        return _output_handler

    # set state and correspond ObserveOutput to action_dict
    def set_action_dict(self, state, action_dict=None):
        action_dict = action_dict if action_dict is not None else {}

        for s in _listify(state):
            if s in action_dict:
                raise Exception(
                    f'{STATE_MAPPING[s]} had already existed in action_dict.')
            action_dict[s] = self

        return action_dict

    # send output to remote master.
    @staticmethod
    def _remote_output(job_obj, fun, *args, **kwargs):
        job_obj.remote_output(
            job_obj.state, fun.__name__, *args, **kwargs)

    # put output handler and variable to exe_output_queue
    @staticmethod
    def _put_output_handler(job_obj, output_observer, *args, **kwargs):
        job_obj.exe_output(output_observer, *args, **kwargs)


class HandleStateChange(object):
    def __init__(self, state_change: Tuple, end=None):
        self.before = state_change[0]
        self.after = state_change[1]
        self.end = end if end is not None else self.after

    def __call__(_self, handler):
        _self.handler = handler
        state_change_key = _state_change_to_key(
            _self.before, _self.after)

        # if handler had already decorated.
        if hasattr(handler, '_state_change'):
            handler._state_change[state_change_key] = _self
            return handler

        # init sate_change dict
        state_change = {}
        state_change[state_change_key] = _self

        def _handle_state_change(self: Job, state_key, before, after, *args, **kwargs):
            # preserve scalability
            handle_state_change_obj = state_change[state_key]

            async def handle_state_change(*args, **kwargs):
                self.LOG.info(
                    f'Handle state change from: {STATE_MAPPING[before]} to {STATE_MAPPING[after]}>, method: < {handler.__name__} >')
                try:
                    if inspect.iscoroutinefunction(handler):
                        await handler(self, *args, **kwargs)
                    else:
                        handler(self, *args, **kwargs)
                except asyncio.CancelledError:
                    self.LOG.warning(
                        f'State Change Handler {handler.__name__} stop running.')
                except Exception:
                    pass

                self.change_state(_self.end)

            self.exe_handler(handle_state_change, *args, **kwargs)

        _handle_state_change._state_change = state_change
        _handle_state_change._handler_name = handler.__name__

        return _handle_state_change


class ActionHandler(object):
    def __init__(self, action, after, require_before=False, cancel_current_task=False):
        assert action in TAKE_ACTION
        self.action = action
        self.after = after
        self.cancel_current_task = cancel_current_task
        self.require_before = require_before

    def __call__(_self, handler):
        _self.handler = handler

        if hasattr(handler, '_action'):
            raise Exception(
                f'{handler.__name__} had already bind with an action.')

        def _action_handler(self: Job, *args, **kwargs):
            before = self.state
            cancel_task = kwargs.pop(
                'cancel_current_task', _self.cancel_current_task)

            # clear exe queue
            if cancel_task is True:
                self.reset_exe_loop()

            action = _self.action
            if self.remote_mode is False:
                async def run_action(*args, **kwargs):

                    if _self.require_before is True:
                        kwargs['before'] = before

                    self.LOG.info(
                        f'Run action <{STATE_MAPPING[action]}>, method: <{handler.__name__}>')
                    if inspect.iscoroutinefunction(handler):
                        await handler(self, *args, **kwargs)
                    else:
                        handler(self, *args, **kwargs)

                    # if sate not change yet after action handler finished.
                    if self.state == action:
                        self.change_state(_self.after)

                # bind action to method run_action
                # when _handler_exe_loop get handler, it will find _action is exist or not.
                # if _action exist, _handler_exe_loop is going to change state depend _action.
                run_action._action = action
                self.exe_handler(
                    run_action, *args, **kwargs)
                return

            # remote mode is True
            if self.remote_role == REMOTE_MATER:
                self.LOG.info(f'Remotely Run action <{STATE_MAPPING[action]}>')
                # The action is already ensured belong to TAKE_ACTION.
                # send action to remote
                self.change_state(action)
            elif self.remote_role == REMOTE_AGENT:
                async def run_action(*args, **kwargs):
                    self.change_state(action)
                    self.LOG.info(f'Run action <{STATE_MAPPING[action]}>')
                    if inspect.iscoroutinefunction(handler):
                        await handler(self, *args, **kwargs)
                    else:
                        handler(self, *args, **kwargs)

                    if self.state == action:
                        self.change_state(_self.after)

                run_action._action = action
                self.exe_handler(run_action, *args, **kwargs)

        _action_handler._action = _self.action
        _action_handler._handler_name = handler.__name__

        return _action_handler


class TaskQueueStopRunning:
    pass


task_stop_obj = TaskQueueStopRunning()


class Job:
    _Job_Types = {}

    def __init__(self, connection=None, timeout=60, state_inform_interval=5, remote_mode=False, remote_role=None):
        self.id = 0
        self.connection: MachineConnection = connection
        self.state = JOB_INIT
        self.remote_mode = remote_mode
        self.handle_task = hub.app_hub.spawn(self._handler_exe_loop)
        self.output_loop = hub.app_hub.spawn(self._output_handler_loop)
        self.timeout = timeout
        self.state_inform_interval = state_inform_interval
        self._handler_exe_queue = asyncio.Queue()
        self._output_queue = asyncio.Queue()
        self.LOG = logging.getLogger(f'Job init')

        if remote_mode is True:
            assert remote_role in (REMOTE_AGENT, REMOTE_MATER)
            self.remote_role = remote_role
        else:
            self.change_state(JOB_CREATED)

    async def _output_handler_loop(self):
        try:
            while True:
                (observer, args, kwargs) = await self._output_queue.get()

                if isinstance(observer, TaskQueueStopRunning):
                    break

                try:
                    await observer(*args, **kwargs)
                except Exception:
                    print(traceback.format_exc())
        except asyncio.CancelledError:
            pass

    async def _handler_exe_loop(self):
        while True:
            try:
                (handler, args, kwargs) = await self._handler_exe_queue.get()

                if isinstance(handler, TaskQueueStopRunning):
                    break

                if hasattr(handler, '_action') and handler._action != self.state:
                    self.change_state(handler._action)

                before = self.state

                await handler(*args, **kwargs)

                after = self.state

                self.LOG.debug(
                    f'State Change after handler {handler.__name__} end:  {STATE_MAPPING[before]} -> {STATE_MAPPING[after]}')

                # if exe_queue is empty, automatically find next step.
                if self._handler_exe_queue.empty():
                    if after in TAKE_ACTION:
                        handler = self._action_set[after]
                        handler(self)
                    elif after in ACTION_RESULT:
                        key = _state_change_to_key(before, after)
                        if key in self._handler_set:
                            for handler in self._handler_set[key]:
                                handler(self, key, before, after)
            except asyncio.CancelledError:
                self.LOG.warning('handler execute loop stop.')

    def reset_exe_loop(self):
        try:
            while self._handler_exe_queue.get_nowait():
                pass
        except asyncio.QueueEmpty:
            pass
        finally:
            if self.handle_task.done() is False:
                self.handle_task.cancel()

    def exe_handler(self, handler, *args, **kwargs):
        self._handler_exe_queue.put_nowait(
            (handler, args, kwargs))

    def exe_output(self, output_handler, *args, **kwargs):
        self._output_queue.put_nowait(
            (output_handler, args, kwargs))

    @classmethod
    def create_job_by_job_info(cls, connection, job_info, job_id, remote_role=None):
        assert job_id > 0

        if isinstance(job_info, str):
            job_info = json.loads(job_info)

        job_type = job_info['job_type']
        job_obj = cls._Job_Types[job_type].create_job_by_job_info(
            job_info, connection, remote_role=remote_role)
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
        self.LOG.name = job_id
        self.id = job_id

    def change_state(self, state):
        if self.state == state:
            return

        if self.remote_mode is True:
            if self.connection is not None:
                self.send_remote_change_state(self.state, state)

            # remote master don't do anything.
            if self.remote_role == REMOTE_MATER:
                return

        self.LOG.name = f'JOB {STATE_MAPPING[state]}'
        self.LOG.info(
            f'State Change: {STATE_MAPPING[self.state]}  -> {STATE_MAPPING[state]}')
        self.state = state

    def send_remote_change_state(self, before, after, info=None):
        msg = MCPJobStateChange(self.connection, self.id,
                                before, after, info)
        self.connection.send_msg(msg)

    def remote_change_state(self, before, after, info=None):
        if self.state == after:
            # if state not change
            return

        # send state change event to job_handler.
        if self.remote_role == REMOTE_MATER:
            self.LOG.info(
                f'State Change From Remote: {STATE_MAPPING[self.state]}  -> {STATE_MAPPING[after]}')
            self.state = after
        elif self.remote_role == REMOTE_AGENT:
            if after in TAKE_ACTION:
                # exe action
                if after in self._action_set:
                    self._action_set[after](self)
                else:
                    self.change_state(after)
            elif after in ACTION_RESULT:
                # raise error
                pass

    def remote_output(self, state, observer_name, *args, **kwargs):
        kwargs['observer_name'] = observer_name
        output = {'args': args, 'kwargs': kwargs}
        msg = MCPJobOutput(self.connection, self.id, state, output)

        self.connection.send_msg(msg)

    def get_remote_output(self, state, info=None):
        args = info['args']
        kwargs = info['kwargs']
        output_handler = self._observe_name_set[kwargs.pop('observer_name')]
        output_handler(self, *args, state=state, **kwargs)

    def run(self):
        pass

    def stop(self):
        pass

    async def delete(self):
        self.reset_exe_loop()
        # send stop obj to both queue.
        self.exe_handler(task_stop_obj)
        self.exe_output(task_stop_obj, 'Delete Task loop.')

        await self._handler_exe_loop
        await self._output_handler_loop

    def job_info_serialize(self, output=None):
        if output is None:
            output = dict()

        output = {
            'job_type': self.cls_job_type,
            'job_id': self.id,
            'job_state': self.state,
            'timeout': self.timeout,
            'state_inform_interval': self.state_inform_interval,
            'remote_mode': self.remote_mode,
        }

        return output
