from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    remote_parallel_map()


my_inputs = list(range(1000))

results = remote_parallel_map(my_function, my_inputs, background=True)

print(results)
