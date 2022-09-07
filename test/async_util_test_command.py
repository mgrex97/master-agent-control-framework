import asyncio

from async_app_fw.lib.hub import app_hub
from custom_app.util.async_command_executor import AsyncCommandExecutor, CommandStop
from custom_app.util.async_utility import EventStateCheckFalied

spawn = app_hub.spawn

if __name__ == '__main__':
    async def run_command():
        cmd_executor = AsyncCommandExecutor('ping 8.8.8.8', name='ping command')
        try:
            get = await cmd_executor.read_stdout()
        except EventStateCheckFalied as e:
            pass

        await cmd_executor.start(exe_timeout=10)
        task = spawn(wait_ping_stop, cmd_executor)

        while await cmd_executor.is_running():
            try:
                get = await cmd_executor.read_stdout()
                print(get, end='')
            except CommandStop as e:
                await asyncio.sleep(2)
        
        await task
        
    async def wait_ping_stop(cmd_executor: AsyncCommandExecutor):
        await cmd_executor.wait_finished(timeout=20)
        print('Command stop.')

    task = app_hub.spawn(run_command)
    app_hub.joinall([task])