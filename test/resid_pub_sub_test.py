import asyncio
from redis_pub_sub.lib import Broker, Publisher, Subscriber, TYPE_PICKLE_OBJ
from async_app_fw.lib.hub import Hub


class A():
    def __init__(self, num) -> None:
        self.num = num


def handle_sub(data):
    print(data.num)


async def pub(broker):

    publisher = Publisher('test', TYPE_PICKLE_OBJ, broker)

    for i in range(10):
        publisher.publish(A(i))
        await asyncio.sleep(2)


async def sub(broker):
    subscriber = Subscriber('test', TYPE_PICKLE_OBJ, broker, timeout=10)
    await subscriber.subscribe(handle_sub)

if __name__ == '__main__':
    hub = Hub()

    async def main():
        broker = Broker(hub)
        await broker.connect()
        p = hub.spawn(pub, broker)
        s = hub.spawn(sub, broker)
        await p
        await s

    task = hub.spawn(main)
    hub.joinall([task])
