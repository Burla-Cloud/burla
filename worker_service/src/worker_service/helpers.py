import queue
import threading
import sys
import traceback
import logging as python_logging
from time import time


class VerboseList(list):
    # simply printing instead of using the python logger causes SEGFAULT errors
    # which kill flask executors, even when we pass flush=True.
    # This is because there are a bunch of different threads using an instance of this class at
    # the same time, python's logging module handles threads a lot better than print does.

    logger = python_logging.getLogger()
    logger.setLevel(python_logging.INFO)
    logger.addHandler(python_logging.StreamHandler(sys.stdout))

    def __init__(self, *a, print_on_append=False, **kw):
        self.start_time = None
        self.print_on_append = print_on_append
        super().__init__(*a, **kw)

    def append(self, item):
        if self.start_time is None:
            self.start_time = time()
        time_since_start = time() - self.start_time
        msg = f"T+{time_since_start:.2f}s: {item}"
        if self.print_on_append:
            self.logger.info(msg)
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


class SizedQueue(queue.Queue):
    # Force user to submit size of their item because it's ususally already available and is slow
    # to calculate for any given generic object, but fast for known objects like input_pkl_with_idx.
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.item_sizes_queue = queue.Queue()
        self.size_bytes = 0

    def put(self, item, size_bytes):
        super()._put(item)
        self.item_sizes_queue.put(size_bytes)
        self.size_bytes += size_bytes

    def _get(self):
        item = super()._get()
        self.size_bytes -= self.item_sizes_queue.get()
        return item

    @property
    def size_gb(self):
        return self.size_bytes / (1024**3)
