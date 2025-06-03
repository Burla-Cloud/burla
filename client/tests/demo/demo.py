from time import sleep, time
from burla import remote_parallel_map


def my_function(my_input):

    sleep(0)

    return my_input


my_inputs = list(range(1))

start = time()
results = remote_parallel_map(my_function, my_inputs)
e2e_runtime = time() - start
print(f"e2e_runtime: {e2e_runtime}")
