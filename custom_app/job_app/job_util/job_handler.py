from async_app_fw.base.app_manager import BaseApp
from custom_app.job_app.job_util.job_class import Job
from custom_app.job_app.job_util.job_event import JobEventBase
from async_app_fw.utils import _listify
from async_app_fw.controller.handler import register_handler_filter

FILTER_TYPE = 2


@register_handler_filter(FILTER_TYPE)
def job_handler_filter(handler, ev: JobEventBase, ev_cls, state):
    if not isinstance(ev, JobEventBase):
        raise TypeError(f'Input variable ev is not instance of JobEventBase.')

    if (callers := getattr(handler, 'callers', None)) is None:
        raise Exception("Job handler doesn't has callers.")

    caller: _JobCaller = callers[ev_cls]

    if ev.job not in caller.job_set:
        return False

    if caller.state is not None:
        if ev.state not in caller.state:
            return False

    return True


class DuplicateJobInJobSet(Exception):
    pass


class _JobCaller():
    def __init__(self, state=None) -> None:
        self.state = state if state is None else _listify(state)
        self.job_set = set()

    def add_job(self, job):
        if job in self.job_set:
            raise DuplicateJobInJobSet()
        self.job_set.add(job)

    def remove_job(self, job):
        self.job_set.remove(job)


def observe_job_event(job_ev, state=None):
    def _observe_job_event(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(job_ev):
            handler.callers[e] = _JobCaller(state)
        return handler
    return _observe_job_event


def config_job_observe(service_brick: BaseApp, job: Job):
    if not isinstance(service_brick, BaseApp):
        raise TypeError(
            f'Input variable service_brick is not instance of BaseApp.')

    if not isinstance(job, Job):
        raise TypeError(f'Input variable job is not instance of Job.')

    job.register_state_change_handler(
        lambda state_change_ev: service_brick.send_event_to_self(state_change_ev))
