import logging

from async_app_fw.lib.hub import app_hub
from async_app_fw.controller.mcp_controller.agent_controller import MachineControlAgentController
from async_util import print_loop_stack

if __name__ == '__main__':
    agent = MachineControlAgentController()

    try:
        task_loop_checking = app_hub.spawn(
            print_loop_stack)  # , print_task=True)
        task = app_hub.spawn(agent.attempt_connecting_loop, 2)
        app_hub.joinall([task, task_loop_checking])
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
