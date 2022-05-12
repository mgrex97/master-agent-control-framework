import logging
from eventlet_framework.base.app_manager import AppManager
from eventlet_framework.lib import hub


def main():
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps(
        ['eventlet_framework.controller.tshark_controller.tshark_handler'])
    contexts = app_mgr.create_contexts()

    services = []
    services.extend(app_mgr.instantiate_apps(**contexts))

    try:
        hub.joinall(services)
    except KeyboardInterrupt:
        logger.debug("Keyboard Interrupt received. "
                     "Closing eventlet framework application manager...")
    finally:
        app_mgr.close()


if __name__ == '__main__':
    main()
