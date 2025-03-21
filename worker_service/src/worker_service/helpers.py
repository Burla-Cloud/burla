import threading
import sys
import traceback
import logging as python_logging


class VerboseList(list):
    # simply printing instead of using the python logger causes SEGFAULT errors
    # which kill flask executors, even when we pass flush=True.
    # This is because there are a bunch of different threads using an instance of this class at
    # the same time, python's logging module handles threads a lot better than print does.

    def __init__(self, *a, **kw):
        self.logger = python_logging.getLogger()
        self.logger.setLevel(python_logging.INFO)
        self.logger.addHandler(python_logging.StreamHandler(sys.stdout))
        super().__init__(*a, **kw)

    def append(self, item):
        self.logger.info(item)
        super().append(item)


class ThreadWithExc(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.traceback_str = None

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            traceback_str = "".join(traceback_details)
            self.traceback_str = traceback_str
