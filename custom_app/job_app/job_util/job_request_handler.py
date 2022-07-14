from async_app_fw.controller.handler import register_handler_filter
from async_app_fw.base.app_manager import BaseApp
from custom_app.job_app.job_util.job_class import Job
from custom_app.job_app.job_util.job_request import JobRequest
from custom_app.job_app.job_util.job_event import JOB_REQUEST_FILTER_TYPE, JobEventBase
from custom_app.job_app.job_util.job_handler import _JobCaller, job_handler_filter
from async_app_fw.utils import _listify


@register_handler_filter(JOB_REQUEST_FILTER_TYPE)
def job_request_url_filter(handler, ev: JobEventBase, ev_cls, state):
    if not isinstance(ev, JobEventBase):
        raise TypeError(f'Input variable ev is not instance of JobEventBase.')

    if (callers := getattr(handler, 'callers', None)) is None:
        raise Exception("Job handler doesn't has callers.")

    caller: _JobCaller = callers[ev_cls]

    if caller.url != None:
        if caller.url != ev.url:
            return False

    if caller.state is not None:
        if ev.state not in caller.state:
            return False

    return True


class _JobRequestCaller(_JobCaller):
    def __init__(self, url=None, state=None):
        super().__init__(state)
        self.url = url


def observe_request_output(job_ev, url=None, state=None):
    def _observe_request_output(handler):
        if 'callers' not in dir(handler):
            handler.callers = {}
        for e in _listify(job_ev):
            handler.callers[e] = _JobRequestCaller(url, state)
        return handler
    return _observe_request_output


def config_observe_job_request_output(service_brick, job_ev_cls, job: JobRequest):
    if not isinstance(service_brick, BaseApp):
        raise TypeError(
            f'Input variable service_brick is not instance of BaseApp.')

    if not issubclass(job_ev_cls, JobEventBase):
        raise TypeError(
            f'Input variable job_ev_cls is not instance of JobEventBase.')

    if not isinstance(job, Job):
        raise TypeError(f'Input variable job is not instance of Job.')

    job.set_output_method(
        lambda output_ev: service_brick.send_event_to_self(output_ev))
