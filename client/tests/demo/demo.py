from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    sleep(1)


my_inputs = list(range(10))


remote_parallel_map(my_function, my_inputs)
