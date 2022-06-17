import asyncio
import inspect
import itertools
import logging
import gc

from async_app_fw.controller.handler import register_instance, get_dependent_services
from async_app_fw.event import event
from async_app_fw.event.event import EventReplyBase, EventRequestBase
from async_app_fw.lib import hub
from async_app_fw.lib.hub import TaskLoop, app_hub
from async_app_fw import utils
from async_app_fw.utils import _listify

LOG = logging.getLogger('async_app_fw.base.app_manager')

SERVICE_BRICKS = {}


def lookup_service_brick(name):
    return SERVICE_BRICKS.get(name)


def _lookup_service_brick_by_ev_cls(ev_cls):
    return _lookup_service_brick_by_mod_name(ev_cls.__module__)


def _lookup_service_brick_by_mod_name(mod_name):
    return lookup_service_brick(mod_name.split('.')[-1])


def register_app(app):
    # Prevent duplicated app registration.
    assert isinstance(app, BaseApp)
    assert app.name not in SERVICE_BRICKS
    SERVICE_BRICKS[app.name] = app
    register_instance(app)


def unregister_app(app):
    SERVICE_BRICKS.pop(app.name)


class EventLoopStop():
    pass


class BaseApp(object):
    _CONTEXT = {}
    _EVENTS = []

    event_loop_stop = EventLoopStop

    @classmethod
    def context_iteritems(cls):
        return iter(cls._CONTEXT.items())

    def __init__(self, *_args, **_kwargs):
        super(BaseApp, self).__init__()
        self.name = self.__class__.__name__
        self.event_handlers = {}
        self.observers = {}
        self.tasks = []
        self._event_loop_task = None
        self.events = hub.Queue()

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

        # prevent accidental creation of instances of this class outside RyuApp
        class _EventThreadStop(event.EventBase):
            pass

        self._event_stop = _EventThreadStop()
        self.is_active = True

    def start(self):
        """
        Hook that is called after startup initialization is done.
        """
        self._event_loop_task = app_hub.spawn(self._event_loop)
        return self._event_loop_task

    def stop(self):
        async def _stop():
            self.is_active = False
            await app_hub.spawn(self._send_event, self.event_loop_stop, None)

            if isinstance(self._event_loop_task, asyncio.Task):
                app_hub.kill(self._event_loop_task)
                await self._event_loop_task

            for task in self.tasks:
                LOG.info(f"App Stop, wait task <{task.get_name()}> stop...")
                app_hub.kill(task)
                await task

        return app_hub.spawn(_stop)

    def register_handler(self, ev_cls, handler):
        assert callable(handler)
        self.event_handlers.setdefault(ev_cls, [])
        self.event_handlers[ev_cls].append(handler)

    def unregister_handler(self, ev_cls, handler):
        assert callable(handler)
        self.event_handlers[ev_cls].remove(handler)
        if not self.event_handlers[ev_cls]:
            del self.event_handlers[ev_cls]

    def register_observer(self, ev_cls, name, states=None):
        states = states or set()
        ev_cls_observers = self.observers.setdefault(ev_cls, {})
        ev_cls_observers.setdefault(name, set()).update(states)

    def unregister_observer(self, ev_cls, name):
        observers = self.observers.get(ev_cls, {})
        observers.pop(name)

    def unregister_observer_all_event(self, name):
        for observers in self.observers.values():
            observers.pop(name, None)

    def observe_event(self, ev_cls, states=None):
        brick = _lookup_service_brick_by_ev_cls(ev_cls)
        if brick is not None:
            brick.register_observer(ev_cls, self.name, states)

    def unobserve_event(self, ev_cls):
        brick = _lookup_service_brick_by_ev_cls(ev_cls)
        if brick is not None:
            brick.unregister_observer(ev_cls, self.name)

    def get_handlers(self, ev, state=None):
        """Returns a list of handlers for the specific event.

        :param ev: The event to handle.
        :param state: The current state. ("dispatcher")
                      If None is given, returns all handlers for the event.
                      Otherwise, returns only handlers that are interested
                      in the specified state.
                      The default is None.
        """
        ev_cls = ev.__class__
        handlers = self.event_handlers.get(ev_cls, [])
        if state is None:
            return handlers

        def test(h):
            if not hasattr(h, 'callers') or ev_cls not in h.callers:
                # dynamically registered handlers does not have
                # h.callers element for the event.
                return True
            states = h.callers[ev_cls].ev_types
            if not states:
                # empty states means all states
                return True
            return state in states

        return filter(test, handlers)

    def get_observers(self, ev, state):
        observers = []
        for k, v in self.observers.get(ev.__class__, {}).items():
            if not state or not v or state in v:
                observers.append(k)

        return observers

    def send_request(self, req):
        """
        Make a synchronous request.
        Set req.sync to True, send it to a Ryu application specified by
        req.dst, and block until receiving a reply.
        Returns the received reply.
        The argument should be an instance of EventRequestBase.
        """

        assert isinstance(req, EventRequestBase)
        req.sync = True
        req.reply_q = hub.Queue()
        self.send_event(req.dst, req)
        # going to sleep for the reply
        return req.reply_q.get()

    async def _event_loop(self):
        try:
            while self.is_active or self.events.empty():
                ev, state = await self.events.get()
                handlers = self.get_handlers(ev, state)
                for handler in handlers:
                    try:
                        handler(ev)
                    except:
                        LOG.exception('%s: Exception occurred during handler processing. '
                                      'Backtrace from offending handler '
                                      '[%s] servicing event [%s] follows.',
                                      self.name, handler.__name__, ev.__class__.__name__)
        except asyncio.CancelledError as e:
            LOG.info(f'App {self.name}, _event_loop been canceled.')

        # clean events queue.
        try:
            while self.events.get_nowait():
                pass
        except asyncio.QueueEmpty:
            pass

    async def _send_event(self, ev, state):
        await self.events.put((ev, state))

    def send_event(self, name, ev, state=None):
        """
        Send the specified event to the RyuApp instance specified by name.
        """

        if name in SERVICE_BRICKS:
            if isinstance(ev, EventRequestBase):
                ev.src = self.name
            LOG.debug("EVENT %s->%s %s",
                      self.name, name, ev.__class__.__name__)
            send_task = app_hub.spawn(
                SERVICE_BRICKS[name]._send_event, ev, state)
            return send_task
        else:
            LOG.debug("EVENT LOST %s->%s %s",
                      self.name, name, ev.__class__.__name__)
            return None

    def send_event_to_observers(self, ev, state=None):
        """
        Send the specified event to all observers of this BaseApp.
        """

        for observer in self.get_observers(ev, state):
            self.send_event(observer, ev, state)

    def reply_to_request(self, req, rep):
        """
        Send a reply for a synchronous request sent by send_request.
        The first argument should be an instance of EventRequestBase.
        The second argument should be an instance of EventReplyBase.
        """

        assert isinstance(req, EventRequestBase)
        assert isinstance(rep, EventReplyBase)
        rep.dst = req.src
        if req.sync:
            req.reply_q.put(rep)
        else:
            self.send_event(rep.dst, rep)

    async def close(self):
        """
        teardown method.
        The method name, close, is chosen for python context manager
        """
        pass


class AppManager(object):
    # singleton
    _instance = None

    @staticmethod
    async def run_apps(app_lists):
        """Run a set of asunc applications

        A convenient method to load and instantiate apps.
        This blocks until all relevant apps stop.
        """
        hub = app_hub
        app_mgr = AppManager.get_instance()
        # load all app which in app_lists. (include dependent app)
        # All of app cls are loaded into app_mgr.applications_cls
        app_mgr.load_apps(app_lists)
        contexts = app_mgr.create_contexts()
        tasks = app_mgr.instantiate_apps(**contexts)
        task_loop = TaskLoop(hub, tasks)
        try:
            await task_loop.wait_tasks()
        finally:
            app_mgr.close()
            # kill all tasks
            for task in tasks:
                hub.kill(task)

            # wait tasks are killed
            for task in tasks:
                await task

            gc.collect()

    @staticmethod
    def get_instance():
        if not AppManager._instance:
            AppManager._instance = AppManager()
        return AppManager._instance

    def __init__(self):
        self.applications_cls = {}
        self.applications = {}
        self.contexts_cls = {}
        self.contexts = {}

    def load_app(self, name):
        mod = utils.import_module(name)
        clses = inspect.getmembers(mod,
                                   lambda cls: (inspect.isclass(cls) and
                                                issubclass(cls, BaseApp) and
                                                mod.__name__ ==
                                                cls.__module__))
        if clses:
            return clses[0][1]
        return None

    def load_apps(self, app_lists):
        # this method will all needed app into self.application_cls
        app_lists = [app for app
                     in itertools.chain.from_iterable(app.split(',')
                                                      for app in app_lists)]
        while len(app_lists) > 0:
            app_cls_name = app_lists.pop(0)

            context_modules = [
                x.__module__ for x in self.contexts_cls.values()]
            # if app already been load.
            if app_cls_name in context_modules:
                continue

            LOG.info('loading app %s', app_cls_name)

            cls = self.load_app(app_cls_name)
            # if can't find target cls.
            if cls is None:
                continue

            self.applications_cls[app_cls_name] = cls

            services = []
            # get service from cls's context, if service not in context_cls yet, call setdefault.
            for key, context_cls in cls.context_iteritems():
                v = self.contexts_cls.setdefault(key, context_cls)
                assert v == context_cls
                context_modules.append(context_cls.__module__)

                if issubclass(context_cls, BaseApp):
                    services.extend(get_dependent_services(context_cls))

            # get service list from cls self.
            # we can't load an app that will be initiataed for
            # contexts.
            for i in get_dependent_services(cls):
                if i not in context_modules:
                    services.append(i)
            # if services not empty
            if services:
                # if the service not in app_lists, append to app_list.
                # set can make sure there is not duplicated service.
                app_lists.extend([s for s in set(services)
                                  if s not in app_lists])

            # these steps can load all dependent Apps which are needed.

    def create_contexts(self):
        for key, cls in self.contexts_cls.items():
            if issubclass(cls, BaseApp):
                # hack for dpset
                context = self._instantiate(None, cls)
            else:
                context = cls()
            LOG.info('creating context %s', key)
            # Prevet duplicated context.
            assert key not in self.contexts
            self.contexts[key] = context
        return self.contexts

    def _update_bricks(self):
        for i in SERVICE_BRICKS.values():
            for _k, m in inspect.getmembers(i, inspect.ismethod):
                if not hasattr(m, 'callers'):
                    continue
                for ev_cls, c in m.callers.items():
                    if not c.ev_source:
                        continue

                    brick = _lookup_service_brick_by_mod_name(c.ev_source)

                    # prevent handler register to self observers dict when the ev_source is self.
                    if i.__module__ == c.ev_source:
                        continue

                    if brick:
                        brick.register_observer(ev_cls, i.name,
                                                c.ev_types)

                    # allow App and Event class are in different module
                    for brick in SERVICE_BRICKS.values():
                        if ev_cls in brick._EVENTS:
                            brick.register_observer(ev_cls, i.name,
                                                    c.ev_types)

    @staticmethod
    def _report_brick(name, app):
        LOG.debug("BRICK %s", name)
        for ev_cls, list_ in app.observers.items():
            LOG.debug("  PROVIDES %s TO %s", ev_cls.__name__, list_)
        for ev_cls in app.event_handlers.keys():
            LOG.debug("  CONSUMES %s", ev_cls.__name__)

    @staticmethod
    def report_bricks():
        for brick, i in SERVICE_BRICKS.items():
            AppManager._report_brick(brick, i)

    def _instantiate(self, app_name, cls, *args, **kwargs):
        # for now, only single instance of a given module
        # Do we need to support multiple instances?
        # Yes, maybe for slicing.
        LOG.info('instantiating app %s of %s', app_name, cls.__name__)

        # check OFP_VERSION
        # if hasattr(cls, 'OFP_VERSIONS') and cls.OFP_VERSIONS is not None:
        #     ofproto_protocol.set_app_supported_versions(cls.OFP_VERSIONS)

        if app_name is not None:
            assert app_name not in self.applications
        app = cls(*args, **kwargs)
        register_app(app)
        assert app.name not in self.applications
        self.applications[app.name] = app
        return app

    def instantiate(self, cls, *args, **kwargs):
        # instaniate app
        # register_app to SERVICE_BRICKS{name:app} and app_mgr.application{name:app},
        # regiser_instance -> register_handler
        app = self._instantiate(None, cls, *args, **kwargs)
        self._update_bricks()
        self._report_brick(app.name, app)
        return app

    def instantiate_apps(self, *args, **kwargs):
        for app_name, cls in self.applications_cls.items():
            self._instantiate(app_name, cls, *args, **kwargs)

        # registor observer
        self._update_bricks()
        self.report_bricks()

        tasks = []
        for app in self.applications.values():
            task_list = _listify(app.start())
            if isinstance(task_list, list):
                tasks.extend(task_list)
        return tasks

    @staticmethod
    async def _close(app):
        close_method = getattr(app, 'close', None)
        if callable(close_method):
            await close_method()

    def uninstantiate(self, name):
        async def _uninstantiate(name):
            app: BaseApp = self.applications.pop(name)
            unregister_app(app)
            for app_ in SERVICE_BRICKS.values():
                app_.unregister_observer_all_event(name)
            await app.stop()
            await self._close(app)
            events = app.events
            if not events.empty():
                app.logger.debug('%s events remains %d',
                                 app.name, events.qsize())

        return app_hub.spawn(_uninstantiate, name)

    async def close(self):
        async def close_all(close_dict):
            for app in close_dict.values():
                await self._close(app)
            close_dict.clear()

        uninstantiate_tasks = []
        for app_name in list(self.applications.keys()):
            uninstantiate_tasks.append(self.uninstantiate(app_name))

        await TaskLoop(app_hub, uninstantiate_tasks).wait_tasks()
        assert not self.applications
        await close_all(self.contexts)
