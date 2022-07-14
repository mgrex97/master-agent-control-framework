from async_app_fw.event.event import EventBase

FILTER_TYPE = 2


class JobEventBase(EventBase):
    FILTER_TYPE = FILTER_TYPE

    def __init__(self, job):
        self.job = job
        self.state = job.state


class JobEventStateChange(JobEventBase):
    def __init__(self, job, previous):
        super().__init__(job)
        self.id = job.id
        self.previous = previous
        self.current = self.state


JOB_REQUEST_FILTER_TYPE = 3


class JobEventRequestOutput(JobEventBase):
    FILTER_TYPE = JOB_REQUEST_FILTER_TYPE

    def __init__(self, job, url, request_info, res):
        super().__init__(job)
        self.url = url
        self.request_info = request_info
        self.res = res
