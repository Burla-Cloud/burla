from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    # print(f"Running input #{my_input} on a remote computer in the cloud!")

    return my_input


my_inputs = list(range(1000001))

return_values = remote_parallel_map(my_function, my_inputs)
