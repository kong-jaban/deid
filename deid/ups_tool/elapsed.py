from datetime import datetime
from ups_tool.util import get_logger
logger = get_logger()

DATE_FORMAT_STRING = '%Y-%m-%d %H:%M:%S'
CNT = 60

class Elapsed:
    def __init__(self, name=None, depth=0) -> None:
        self.depth = depth
        self._name = name
        self._start = datetime.now()
        self._print_start(name)

    _dash = lambda self: ('=' if self.depth == 0 else '-') * 60

    def _print_start(self, msg='') -> None:
        logger.info(('\t'*self.depth) + self._dash())
        logger.info(('\t'*self.depth) + f'start: {msg}')
        logger.info(('\t'*self.depth) + self._dash())

    def print_elapsed(self, msg='') -> None:
        current = datetime.now()
        logger.info(('\t'*self.depth) + self._dash())
        logger.info(('\t'*self.depth) + f'elapsed: {self._name}: {current - self._start}: {msg}')
        logger.info(('\t'*self.depth) + self._dash())
