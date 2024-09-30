from burla import remote_parallel_map
from time import time


def my_function(my_input):
    print(f"hi #{my_input}")
    return my_input * 2


inputs = list(range(200))

start = time()

results = remote_parallel_map(my_function, inputs)
results = list(results)

print(f"Done after {time() - start}s")
print(f"num results: {len(results)}")
print(results)
