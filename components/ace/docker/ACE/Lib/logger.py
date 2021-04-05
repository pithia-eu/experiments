import os, re
import time
from datetime import datetime, timedelta
import logging
import logging.handlers


class ACELogger(object):
    @property
    def logger(self):
        if not self._logger:
            self.setLogger()
        return self._logger

    def setLogger(self, ):
        # create logger
        logger = logging.getLogger('ACE')
        logger.setLevel(logging.DEBUG)

        # create RQ handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter and add it to the handlers
        formatter = logging.Formatter(fmt='%(levelname)s - %(message)s - %(asctime)s', datefmt='%H:%M:%S')
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(ch)
        self._logger = logger

    def __init__(self, task=None):
        self._logger = None