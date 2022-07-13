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
