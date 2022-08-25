from async_app_fw.base.app_manager import AppManager
from . import AsyncTestApp

app_manager = AppManager.get_instance()

async def install_test_app(app_cls, name, timeout=20):
    app_instance:AsyncTestApp = app_manager.instantiate(app_cls, name, timeout=timeout)

    app_instance.start()

    res = await app_instance.wait_test_end()

    await app_instance.close()

    return res