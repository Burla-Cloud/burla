from threading import Event
from queue import PriorityQueue, Empty
from time import sleep
from datetime import datetime, timezone

from yaspin import Spinner


def print_logs_from_queue(log_queue: PriorityQueue, stop_event: Event, spinner: Spinner):
    last_log_timestamp = None
    last_log_printed_at = None
    is_first_log = True
    while True:
        try:
            timestamp, log_message = log_queue.get_nowait()

            if is_first_log:
                sleep(6)  # remote_parallel_map.JOB_STATUS_POLL_RATE_SEC
                is_first_log = False

            current_epoch = datetime.now(timezone.utc).timestamp()

            last_log_printed_delta = current_epoch - (last_log_printed_at or current_epoch)
            last_log_occurred_delta = timestamp - (last_log_timestamp or timestamp)
            sleep_time = max(last_log_occurred_delta - last_log_printed_delta, 0)

            # if last_log_occurred_delta - last_log_printed_delta < 0:
            #     lateness = (last_log_occurred_delta - last_log_printed_delta) * -1
            #     print(f"LATE!! Log should have printed {lateness} seconds ago!")

            sleep(sleep_time)
            spinner.write(log_message)
            last_log_printed_at = datetime.now(timezone.utc).timestamp()
            last_log_timestamp = timestamp
        except Empty:
            if stop_event.is_set():
                return
            sleep(0.5)
