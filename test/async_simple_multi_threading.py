import asyncio
from curses import KEY_A1
from async_app_fw.lib.hub import Hub
from concurrent.futures import ThreadPoolExecutor


hub_consumer = Hub()
hub_provider = Hub()

queue = asyncio.Queue()


async def consumer(q: asyncio.Queue):
    while True:
        print('consume')
        get = await hub_provider.run_corotine_threadsafe(q.get)
        print(f'get {get}')


async def provider(q: asyncio.Queue):
    i = 0
    while True:
        print('provide')
        await asyncio.sleep(1)
        await hub_consumer.run_corotine_threadsafe(q.put, 'haha')
        if (i := i + 1) == 10:
            break


def threading_event_loop_entrance(coro_function, queue, hub: Hub):
    hub.loop = asyncio.new_event_loop()
    task = hub.spawn(coro_function, queue)
    hub.joinall([task])


if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=2) as executor:
        try:
            future_p = executor.submit(
                threading_event_loop_entrance, provider, queue, hub_provider)
            future_c = executor.submit(
                threading_event_loop_entrance, consumer, queue, hub_consumer)
        except KeyboardInterrupt:
            pass
            # future_p.cancel()
            # future_c.cancel()
