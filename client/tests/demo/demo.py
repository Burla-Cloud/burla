from time import time, sleep

from burla import remote_parallel_map


def simple_test_function(my_input):
    sleep(1)


my_inputs = list(range(1_000_000))


remote_parallel_map(simple_test_function, my_inputs)
