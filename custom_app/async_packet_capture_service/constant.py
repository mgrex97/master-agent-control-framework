from custom_app.util.async_tshark import AsyncLiveCaptureService

CAPTURE_SERVCIE_AGENT_HANDLER_APP_NAME = 'capture_service_agent_handler'
CAPTURE_SERVCIE_MASTER_HANDLER_APP_NAME = 'capture_service_master_handler'

SERVICE_CLOSE_MONITER_INTERVAL = 10

CAPTURE_SERVICE_CLS_ID_MAPPING = {
    AsyncLiveCaptureService.capture_cls_id: AsyncLiveCaptureService
}