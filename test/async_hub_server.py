import asyncio
from async_app_fw.controller.mcp_controller.master_controller import MachineControlMasterController
from async_app_fw.lib import hub
from async_app_fw.lib.hub import app_hub
from async_util import print_loop_stack

if __name__ == '__main__':
    master = MachineControlMasterController()

    try:
        task1 = hub.app_hub.spawn(
            print_loop_stack, loop=app_hub.loop, interval=3)
        task2 = hub.app_hub.spawn(master.server_loop)
        hub.app_hub.joinall([task1, task2])
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
