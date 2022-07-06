import traceback
import json
import asyncio
import async_timeout
import pickle
from redis import Redis
import redis.asyncio as redis
from redis.asyncio.client import PubSub
from async_app_fw.lib.hub import Hub
from async_app_fw.utils import _listify

TYPE_PICKLE_OBJ = '0'
TYPE_JSON = '1'
TYPE_STRING = '2'

_type_serialize_map = {
    '0': lambda obj: pickle.dumps(obj),
    '1': lambda obj: json.dumps(obj),
    '2': lambda obj: obj
}

_type_deserialize_map = {
    '0': lambda obj: pickle.loads(obj),
    '1': lambda obj: json.loads(obj),
    '2': lambda obj: obj.decode(encoding='utf-8')
}


class Broker():
    def __init__(self, hub) -> None:
        # wait to implement:
        # singal tone
        # connection error handler
        self.hub: Hub = hub
        self.rdb = redis.ConnectionPool(
            connection_class=redis.UnixDomainSocketConnection, path="/var/run/redis/redis.sock")

        self.connection = None
        self.pub_queue = asyncio.Queue()
        self.sub_callback_dict = {}

    async def connect(self):
        self.connection: Redis = await redis.Redis(connection_pool=self.rdb)
        self._pub_send_task = self.hub.spawn(self._pub_send_loop)

    async def _pub_send_loop(self):
        # improve dot operator
        connection = self.connection
        pub_queue = self.pub_queue

        while True:
            (path, data, data_type) = await pub_queue.get()
            path_act = path + '/' + data_type

            # serialize data
            try:
                serialize_data = _type_serialize_map[data_type](data)
            except Exception as e:
                print('public serialize error')
                print(traceback.format_exc())
                continue

            await connection.publish(path_act, serialize_data)

    async def _sub_read_loop(self, pubsub: PubSub, path, data_type, callback, timeout=5):
        deserialize = _type_deserialize_map[data_type]
        callback = _listify(callback)
        final_path = path + '/' + data_type

        # wait subscribe end.
        await pubsub.subscribe(final_path, final_path=callback)

        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)

                if message is None:
                    continue

                if (origin_data := message.get('data', None)) is None:
                    continue

                data = deserialize(origin_data)
                for fun in callback:
                    try:
                        fun(data)
                    except Exception as e:
                        print(
                            f'callback error name [{fun.__name__}\n{traceback.format_exc()}')
        except asyncio.TimeoutError as e:
            raise e
        except Exception as e:
            print(
                f'deserialize error :\n {traceback.format_exc()}')

    def publish(self, path, data, data_type):
        # push to pub queue
        self.pub_queue.put_nowait((path, data, data_type))

    async def subscribe(self, path, data_type, callback, timeout) -> asyncio.Task:

        # register callback function to sub_callback_dict
        callback_dict = self.sub_callback_dict.setdefault(path, {})
        callback_dict[callback.__name__] = callback

        pubsub = self.connection.pubsub()

        return self.hub.spawn(
            self._sub_read_loop, pubsub, path, data_type, callback, timeout), path


class Publisher():
    def __init__(self, path, data_type, broker: Broker) -> None:
        self.path = path
        self.data_type = data_type
        self.broker: Broker = broker
        self.publish = lambda data: broker.publish(path, data, data_type)


class SubscriberStillRunning(Exception):
    pass


class SubscriberReaderNotRunning(Exception):
    pass


class Subscriber():
    def __init__(self, path, data_type, broker, timeout=None) -> None:
        self.broker: Broker = broker
        self.path = path
        self.data_type = data_type
        self.timeout = timeout
        self._sub_read_task = None

    async def subscribe(self, callback):
        if isinstance(self._sub_read_task, asyncio.Task):
            raise SubscriberStillRunning(
                f'Subscriber read task on path [{self.path}] is still running.')

        self._sub_read_task = await self.broker.subscribe(self.path, self.data_type, callback, self.timeout)
        return self._sub_read_task

    async def stop_subscribe(self):
        if not isinstance(self._sub_read_task, asyncio.Task):
            raise SubscriberReaderNotRunning()

        self._sub_read_task.cancel()
        await self._sub_read_task()
        self._sub_read_task = None
