from time import sleep
from burla import remote_parallel_map


def my_function(my_input):
    # Something intense goes here!
    sleep(0.1)


remote_parallel_map(my_function, list(range(10_000_000)))
