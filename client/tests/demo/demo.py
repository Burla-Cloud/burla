from time import time, sleep

from burla import remote_parallel_map


def my_function(my_input):

    sleep(1)

    return my_input


my_inputs = list(range(30))


start = time()

list_of_return_values = remote_parallel_map(my_function, my_inputs)

print(f"Time taken: {time() - start} seconds")
