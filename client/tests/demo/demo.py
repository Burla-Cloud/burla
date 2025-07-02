from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    sleep(0.1)

    return my_input  # list(range(100000))


my_inputs = list(range(1000))

result_generator = remote_parallel_map(my_function, my_inputs, generator=True)

for result in result_generator:
    pass
