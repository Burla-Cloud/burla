from burla import remote_parallel_map
from time import time


def my_function(my_input):

    called_at = time()
    return called_at


inputs = list(range(512))

start = time()
print(f"STARTED AT: {start}")
results = remote_parallel_map(my_function, inputs)
results = list(results)

print(f"Done after {time() - start}s")
print(f"num results: {len(results)}")
print(results)
