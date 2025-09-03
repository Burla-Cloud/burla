from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(1000))


def my_function(my_input):
    sleep(1)
    print(f"Running Input #{my_input} on it's own separate computer in the cloud!")


remote_parallel_map(my_function, my_inputs)
