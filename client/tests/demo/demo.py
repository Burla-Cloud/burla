from time import sleep
from burla import remote_parallel_map


def my_function(my_input):
    print(f"Running {my_input} in it's own separate container in the cloud!")
    sleep(1)
    return my_input * 2


my_inputs = list(range(1000))

results = remote_parallel_map(my_function, my_inputs)

print(results)
