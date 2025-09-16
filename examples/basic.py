# from time import sleep
import os
from burla import remote_parallel_map


def my_function(x):

    print(os.getcwd())

    print(f"hi #{x}")


remote_parallel_map(my_function, list(range(10)))
