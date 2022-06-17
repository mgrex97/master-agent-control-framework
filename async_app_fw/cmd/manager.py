import logging
from async_app_fw.base.app_manager import AppManager
from async_app_fw.lib import hub
from async_app_fw.cfg import CONF


def main():
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger(__name__)
    app_mgr = AppManager.get_instance()
    app_mgr.load_apps(
        ['async_app_fw.controller.mcp_controller.mcp_handler'])
    # ['async_app_fw.controller.tshark_controller.tshark_handler', 'evetlet_framwork.controller.mcp_controller.mcp_handler'])

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
