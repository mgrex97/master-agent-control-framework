import asyncio


async def print_loop_stack(loop=None, print_task=False, interval=5):
    try:
        while True:
            if loop is None:
                loop = asyncio.get_event_loop()

            tasks = asyncio.all_tasks(loop=loop)
            print(f'--------- Task list len: {len(tasks)} ----------')
            if print_task is True:
                print('----------- Task List Detail --------------')
                for task in tasks:
                    print('Remain Task:', task.get_name())
                    if len(tasks) == 2:
                        print(task)
                print('----------------- end ---------------------')
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        return
