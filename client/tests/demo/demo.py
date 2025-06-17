from time import sleep
from burla import remote_parallel_map


def my_function(my_input):
    return my_input * 2


my_inputs = list(range(100000))

results = remote_parallel_map(my_function, my_inputs)

print(results)
