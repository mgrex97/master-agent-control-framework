from enum import Enum

class AsyncServiceEventID(Enum):
    " Default async utility's event name "
    start = 0
    prepare = 1
    running = 2
    # When stop event is not setted, it mean AsyncService is running.
    stop = 3
    finish = 4

EVENT_NAMES_MAPPING = {
    AsyncServiceEventID.start.value: 'start',
    AsyncServiceEventID.prepare.value: 'prepare',
    AsyncServiceEventID.running.value: 'running',
    AsyncServiceEventID.stop.value: 'stop',
    AsyncServiceEventID.finish.value: 'finish'
}
