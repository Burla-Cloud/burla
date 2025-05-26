from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    sleep(0)


my_inputs = list(range(1000))


remote_parallel_map(my_function, my_inputs)
