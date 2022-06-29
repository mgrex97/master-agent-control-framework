import asyncio
import logging

from async_app_fw.base.app_manager import AppManager, lookup_service_brick
from async_app_fw.lib.hub import TaskLoop, app_hub
from custom_app.job_app.job_master_handler import JobMasterHandler
from custom_app.job_app.job_util.api_action_module import SwitchAPIAction
from custom_app.job_app.job_util.job_class import REMOTE_MATER
from custom_app.job_app.job_util.job_request import JobRequest
from async_util import print_loop_stack


async def test_job(job_master_handler: JobMasterHandler):
    # logging.basicConfig(level=logging.INFO)
    await asyncio.sleep(2)

    switch_api = SwitchAPIAction()
    login_info = switch_api.get_login_info()

    request_info_lldp = {
        'running_times': 5,
        'retry_mode': True,
        'retry_data': {
            'type': 'get',
            'url': 'http://10.88.0.14/api/v1/lldp/neighbors/status'
        },
        'host_ip': login_info['host_ip'],
        'login_info': login_info
    }

    job1 = JobRequest(request_info=request_info_lldp,
                      remote_mode=True, remote_role=REMOTE_MATER)

    request_info_rstp_role = {
        'running_times': 100,
        'retry_mode': True,
        'retry_data': {
            'type': 'get',
            'url': 'https://10.88.0.14/api/v1/rstp/interface/role'
        },
        'host_ip': login_info['host_ip'],
        'login_info': login_info
    }

    job2 = JobRequest(request_info=request_info_rstp_role,
                      remote_mode=True, remote_role=REMOTE_MATER)

    job_master_handler.install_job(job1, '127.0.0.1')
    job_master_handler.install_job(job2, '127.0.0.1')
    await asyncio.sleep(3)
    job1.run()
    job2.run()
    # self.clear_job('127.0.0.1')
    await asyncio.sleep(3)
    job1.delete()
    job2.delete()


async def application_init_and_run():
    app_hub.setup_eventloop()

    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        # 'async_app_fw.controller.mcp_controller.master_handler',
        'custom_app.job_app.job_master_handler'
    ])

    contexts = app_mgr.create_contexts()

    services = []
    services.extend(app_mgr.instantiate_apps(**contexts))
    job_app = lookup_service_brick('job_master_handler')
    # services.append(app_hub.spawn(
    # print_loop_stack, loop=app_hub.loop, print_task=True))
    services.append(app_hub.spawn(test_job, job_app))

    task_loop = TaskLoop(app_hub, services)
    try:
        await task_loop.wait_tasks()
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        pass
        await app_mgr.close()

if __name__ == '__main__':
    task = app_hub.spawn(application_init_and_run)
    app_hub.joinall([task])
