import asyncio
from typing import Dict
import aiohttp
import traceback
from async_app_fw.base.app_manager import AppManager
import logging
from async_app_fw.lib.hub import app_hub
from custom_app.util.async_command_executor import AsyncCommandExecutor
from custom_app.util.async_api_action import POST, APIAction, encrpt_password

spawn = app_hub.spawn

agent_to_sw = {
    '169.254.0.1': '10.88.0.11',
    '169.254.0.2': '10.88.0.12',
    '169.254.0.3': '10.88.0.14',
    '169.254.0.4': '10.88.0.13',
}

async def api_action_test():
    logging.basicConfig(level=logging.INFO)
    await asyncio.sleep(5)

    try:
        print('start')
        coros = []
        api_actions: Dict[str, APIAction] = {}
        for agent_ip, sw_ip in agent_to_sw.items():
            base_url=f'https://{sw_ip}/api/v1/'
            login_info={
                'url': 'users/login',
                'method': POST,
                'data': {
                    'username': 'admin',
                    'password': encrpt_password('qnap1234'),
                },
                'auth_path': 'result'
            }
 
            api_actions[agent_ip] = api_action = APIAction(base_url=base_url, login_info=login_info)
            coros.append(api_action.login_api(agent=agent_ip))

        print('login api and create api_action')
        await asyncio.gather(*coros)

        port_state: Dict[str, aiohttp.ClientResponse] = {}
        
        print('get port state')
        for agent_ip in agent_to_sw.keys():
            port_state[agent_ip] = (await api_actions[agent_ip].get('ports', agent=agent_ip)).json['result']
            # print(f'Get {port_state[agent_ip]} from {api_actions[agent_ip].base_url}')

        new_config = []
        coros = []

        for agent_ip , configs in port_state.items():
            api_action: APIAction = api_actions[agent_ip]
            new_config.clear()
            for config in configs:
                if config['val']['Shutdown'] is False:
                    continue
                config['val']['Shutdown'] = False

                new_config.append({'idx': str(config['key']), 'data': config['val']})
 
            if len(new_config) > 0:
                coros.append(api_action.put('portlist', new_config.copy(), agent=agent_ip))

        print('set port config')
        if len(coros) > 0:
            await asyncio.gather(*coros)

    except Exception as e:
        print(traceback.format_exc())
    
async def wait_cmd_stop(cmd: AsyncCommandExecutor):
    await cmd.wait_finished(timeout=20)
    cmd._check_exception()
    print('Tshark stop.')

async def application_init_and_run():
    app_hub.setup_eventloop()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps([
        'custom_app.api_action_app.api_action_master_handler'
    ])

    contexts = app_mgr.create_contexts()

    app_mgr.instantiate_apps(**contexts)
    services = []
    services.append(spawn(api_action_test))
    # services.append(app_hub.spawn(print_loop_stack,
    # loop=app_hub.loop, print_task=True))

    try:
        await asyncio.wait(services)
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        pass
        await app_mgr.close()

if __name__ == '__main__':
    task = app_hub.spawn(application_init_and_run)
    app_hub.joinall([task])