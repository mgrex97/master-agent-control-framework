import logging
from queue import Empty
from async_app_fw.controller.mcp_controller.mcp_controller import MachineConnection
from custom_app.job_app.job_class import JOB_ASYNC, JOB_DELETE, Job
from async_app_fw.lib import hub
from async_app_fw.lib.hub import TaskLoop, app_hub


LOG = logging.getLogger('custom_app.job_app.job_manager')


"""
class ThreadCommunicateQueue:
    def __init__(self):
        self.q_send = hub.Queue(maxsize=1)
        self.q_back = hub.Queue(maxsize=1)

    def send(self, item, block=False):
        self.q_send.put(item, block=block)

    def send_back(self, item, block=False):
        self.q_back.put(item, block=block)

    def wait_send(self):
        while True:
            try:
                item = self.q_send.get(block=False)
                LOG.info(f'Get item from Queue. Item {item}')
                return item
            except Empty:
                hub.sleep(0.1)

    def wait_back(self):
        while True:
            try:
                item = self.q_back.get(block=False)
                LOG.info(f'Get item from Queue. Item {item}')
                return item
            except Empty:
                hub.sleep(0.1)


class JobCreateComm(ThreadCommunicateQueue):
    def __init__(self):
        self.q_send = hub.Queue(maxsize=1)
        self.q_back = hub.Queue(maxsize=1)
        self.check = False

    def set_xid(self, xid):
        self.xid = xid
        self.check = False

    def send_back(self, item, block=False):
        self.check = True
        return super().send_back(item, block)
"""


class JobManager:
    def __init__(self, connection=None):
        self.connection = connection
        if connection is not None:
            self.address = connection.address
        else:
            self.address = None
        self.__job_serial_id = 1
        self.job_request = {}
        self.jobs = {}

    def add_request(self, xid, job_obj: Job):
        self.job_request[xid] = job_obj
        if job_obj.connection is None:
            job_obj.connection = self.connection

    def add_job(self, job_obj):
        assert job_obj.id is not None
        assert job_obj.id > 0
        self.jobs[job_obj.id] = job_obj

    def job_id_async(self, xid, job_id):
        assert job_id not in self.jobs
        job: Job = self.job_request.get(xid, None)
        assert job is not None
        self.jobs[job_id] = job
        job.change_state(JOB_ASYNC)

    def del_job(self, job_id):
        self.jobs[job_id].change_state(JOB_DELETE)
        job: Job = self.jobs.pop(job_id)
        return job.stop()

    def job_state_inform(self, msg):
        logging.info(f'get inform, job_id: {msg.job_id}')

    def job_request_job(self, job_id):
        return self.job_request.get(job_id, None)

    def get_job(self, job_id):
        return self.jobs.get(job_id, None)

    def run_job(self, job_id):
        job: Job = self.jobs.get(job_id, None)
        assert job is not None
        assert job.id != 0

        job.run_job()

    def get_new_job_id(self):
        job_id = self.__job_serial_id
        self.__job_serial_id = self.__job_serial_id + 1
        return job_id

    def delete_all_job(self):
        for job_id in list(self.jobs.keys()):
            self.del_job(job_id)

    def delete_job(self, job_id):
        job: Job = self.jobs.pop(job_id)
        del job
