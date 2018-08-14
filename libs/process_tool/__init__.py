import logging
logger = logging.getLogger(__name__)

import fcntl


class LockMe:
    def __init__(self):
        self._file = None

    def try_lock(self):
        try:
            if self._file is None:
                self._file = open(__file__, 'a')

            fcntl.lockf(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError as e:
            self._safe_close()
            logger.info('i am locked')
            return False

    def unlock(self):
        if self._file is None:
            return

        fcntl.flock(self._file, fcntl.LOCK_UN)
        self._safe_close()
        logger.info('I am unlocked.')

    def _safe_close(self):
        if self._file is not None:
            self._file.close()
            self._file = None
