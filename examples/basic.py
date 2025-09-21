from time import sleep
from burla import remote_parallel_map


def my_function(x):

    sleep(1)

    print(f"I'm running on my own separate computer in the cloud! #{x}")


remote_parallel_map(my_function, list(range(1000)))
