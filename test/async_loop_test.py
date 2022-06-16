import asyncio
from eventlet_framework.lib.async_hub import app_hub, TaskLoop


async def test(n, x):
    print(f"Start :{n} - {x}: sleep {n-x}")
    await asyncio.sleep(n - x)
    print(f"End :{n} - {x}: sleep {n-x}")


async def test_for_loop(n):
    tasks = []
    for x in range(n):
        task = app_hub.spawn(test, n, x)
        task.set_name(f'{n} : {x}')
        tasks.append(task)

    taskloop = TaskLoop(app_hub, tasks)

    await taskloop.wait_tasks()

app_hub.loop.run_until_complete(test_for_loop(10))
