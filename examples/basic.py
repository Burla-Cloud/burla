from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(100))


def my_function5(my_input):
    sleep(1)
    print(f"Running Input #{my_input} on it's own separate computer in the cloud!")


# remote_parallel_map(my_function2, my_inputs)
remote_parallel_map(my_function5, my_inputs)
