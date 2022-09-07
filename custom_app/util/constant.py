from enum import Enum

class AsyncUtilityEventID(Enum):
    " Default async utility's event name "
    start = 0
    prepare = 1
    running = 2
    # When stop event is not setted, it mean AsyncUtility is running.
    stop = 3
    finish = 4

EVENT_NAMES_MAPPING = {
    AsyncUtilityEventID.start.value: 'start',
    AsyncUtilityEventID.prepare.value: 'prepare',
    AsyncUtilityEventID.running.value: 'running',
    AsyncUtilityEventID.stop.value: 'stop',
    AsyncUtilityEventID.finish.value: 'finish'
}
