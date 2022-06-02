from eventlet_framework.lib import hub
import json

# job state
JOB_CREATE = 1
JOB_ASYNC = 2
JOB_RUNING = 3
JOB_DELETE = 4

# job type
CMD_JOB = 1


class Job:
    _Job_Types = {}

    def __init__(self, connection, timeout=60, state_inform_interval=5):
        self.id = 0
        self.state = JOB_CREATE
        self.connection = connection
        self.timeout = timeout
        self.state_inform_interval = state_inform_interval

    @classmethod
    def create_job_by_job_info(cls, connection, job_info, job_id):
        assert job_id > 0

        if isinstance(job_info, str):
            job_info = json.loads(job_info)

        job_type = job_info['job_type']
        job_obj = cls._Job_Types[job_type].create_job_by_job_info(
            connection, job_info)
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
        self.id = job_id

    def change_state(self, state):
        self.state = state

    def parse(self, payload):
        pass

    def run_job(self):
        self.change_state(JOB_RUNING)

    def job_info_serialize(self, output=None):
        if output is None:
            output = dict()

        output = {
            'job_type': self.cls_job_type,
            'job_id': self.id,
            'job_state': self.state,
            'timeout': self.timeout,
            'state_inform_interval': self.state_inform_interval
        }

        return output


@Job.register_job_type(CMD_JOB)
class JobCommand(Job):
    def __init__(self, connection, command, timeout=60, state_inform_interval=5):
        super().__init__(connection, timeout, state_inform_interval)
        self.command = command
        self.proc = None
        self.stdin = None
        self.stdout = None
        self.stderr = None

    @ classmethod
    def create_job_by_job_info(cls, connection, job_info):
        return cls(connection, job_info['cmd_job_info']['command'],
                   job_info['timeout'], job_info['state_inform_interval'])

    def job_info_serialize(self):
        output = super().job_info_serialize()
        output['cmd_job_info'] = {
            'command': self.command
        }
        return json.dumps(output)

    def run_job(self):
        super().run_job()
        self.run_command()

    def run_command(self):
        self.__process = hub.run_command(self.command)
        self.stdin = self.__process.stdin
        self.stdout = self.__process.stdout
        self.stderr = self.__process.stderr
        self.state = True
        self.get_stdout()

    def get_stdout(self):
        hub.spawn(self.__read_output_from_green_io, self.stdout)

    def get_stderr(self):
        hub.spawn(self.__read_output_from_green_io, self.stderr)

    def __read_output_from_green_io(self, greenpipe):
        while True:
            get = greenpipe.readline()

            if self.__process.poll() is not None and len(get) == 0:
                break

            hub.sleep(0)

            output_info = super(JobCommand, self).job_info_serialize()
            output_info['output'] = get.decode(encoding='utf-8')

            output_msg = self.connection.mcproto_parser.MCPJobOutput(
                self.connection, self.id, json.dumps(output_info))
            self.connection.send_msg(output_msg)

    def __get_stderr(self):
        pass

    def wait_job_end(self):
        self.__process.wait()
        self.state = False
