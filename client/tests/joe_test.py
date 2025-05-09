from burla import remote_parallel_map
import os


my_arguments = [1, 2, 3, 4, 89, 949649, 9539053]


def my_function(my_argument: int):
    print(f"Running on remote computer #{my_argument} in the cloud!")
    return my_argument * 2


results = remote_parallel_map(my_function, my_arguments)

print(f"return values: {list(results)}")
