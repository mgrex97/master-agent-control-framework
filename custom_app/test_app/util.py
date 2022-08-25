import logging
from async_app_fw.base.app_manager import AppManager
from . import AsyncTestApp

app_manager = AppManager.get_instance()

LOG = logging.getLogger("Test App Module")

async def install_test_app(app_cls, name, timeout=20):
    LOG.info(f'Instantiate Test App <{name}>.')

    app_instance:AsyncTestApp = app_manager.instantiate(app_cls, name, timeout=timeout)

    LOG.info(f'Start Test App <{name}>.')
    app_instance.start()

    res = await app_instance.wait_test_end()

    LOG.info(f'Test App <{name}> stop running.')

    await app_instance.close()

    return res