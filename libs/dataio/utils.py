import logging
logger = logging.getLogger(__name__)

import os
import sys


def set_mtime(filename, mtime):
    t = int(mtime.timestamp())
    os.utime(filename, (t, t))


def xlearning_progress(epoch, ratio):
    print(f'reporter progress:{ratio}', file=sys.stderr)
    logger.info('finish epoch %d, %.2f', epoch, ratio)
