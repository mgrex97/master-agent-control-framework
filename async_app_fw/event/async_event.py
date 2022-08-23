import asyncio
from async_app_fw.base.app_manager import BaseApp, lookup_service_brick
from async_app_fw.event.event import EventRequestBase
from async_app_fw.lib.hub import app_hub

spawn = app_hub.spawn

class RequestAlreadySend(Exception):
    pass

class RequestTimeout(Exception):
    pass

class RequestAreNotWaitingReply(Exception):
    pass

class RequestCantSend(Exception):
    pass

class RequestStateError(Exception):
    pass

REQ_NOT_SEND = 'not_send'
REQ_SPAWN_SEND = 'spawn_send_task'
REQ_WAIT_REPLY = 'wait_reply'
REQ_FINISHED = 'req_finished'

async def _async_send_request(req):
    if req.req_state != REQ_SPAWN_SEND:
        raise RequestStateError()

    # find target application service and send request.
    service:BaseApp = lookup_service_brick(req.dst)
    service.send_event_to_self(req)

    req.update_state(REQ_WAIT_REPLY)
    try:
        res = await asyncio.wait_for(req.reply_q.get(), timeout=req.timeout)
    except asyncio.TimeoutError as e:
        raise e

    if isinstance(res, Exception):
        raise res

    return res

class EventAsyncRequestBase(EventRequestBase):
    REQUEST_NAME = 'Async Request Base'
    DST_NAME = None

    def __init__(self, timeout=None):
        super().__init__()
        self.dst = self.DST_NAME 
        self.timeout = timeout
        self.req_state = REQ_NOT_SEND

    @classmethod
    def send_request(cls, *args, timeout=None, app_name=None, **kwargs):
        req = cls(*args, **kwargs , timeout=timeout)

        if app_name is not None:
            # update request's destination app.
            req.dst = app_name

        if req.req_state != REQ_NOT_SEND:
            raise RequestAlreadySend(f'Request <{cls.REQUEST_NAME}> has sent.')

        req.update_state(REQ_SPAWN_SEND)

        return spawn(_async_send_request, req)

    def push_reply(self, reply_data):
        if self.req_state is not REQ_WAIT_REPLY:
            raise RequestAreNotWaitingReply()

        self.reply_q.put_nowait(reply_data)
        self.update_state(REQ_FINISHED)

    def update_state(self, state):
        self.req_state = state
