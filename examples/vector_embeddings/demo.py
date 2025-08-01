from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(1000))


def my_function(my_input):
    sleep(1)
    print(f"I'm running on my own separate computer in the cloud! #{my_input}")


remote_parallel_map(my_function, my_inputs)
