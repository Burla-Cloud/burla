from threading import Event
from queue import PriorityQueue, Empty

from yaspin import Spinner


def print_logs_from_queue(log_queue: PriorityQueue, stop_event: Event, spinner: Spinner):
    while True:
        try:
            _, log_message = log_queue.get_nowait()
            spinner.write(log_message)
        except Empty:
            if stop_event.is_set():
                return
