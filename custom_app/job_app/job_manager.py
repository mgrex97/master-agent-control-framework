import logging
from custom_app.job_app.job_class import JOB_ASYNC, Job
from eventlet_framework.lib import hub


class JobManager:
    def __init__(self, connection):
        self.connection = connection
        self.address = connection.address
        self.__job_serial_id = 0
        self.job_request = {}
        self.jobs = {}

    def add_request(self, xid, job_obj):
        self.job_request[xid] = job_obj

    def add_job(self, job_obj):
        assert job_obj.job_id is not None
        assert job_obj.job_id > 0
        self.jobs[job_obj.job_id] = job_obj

    def job_id_async(self, xid, job_id):
        assert job_id not in self.jobs
        job: Job = self.job_request.get(xid, None)
        assert job is not None
        job[job_id] = job
        job.change_state(JOB_ASYNC)

    def del_job(self, job_id):
        del self.jobs[job_id]

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
        self.__job_serial_id = self.job_serial_id + 1
        return job_id
