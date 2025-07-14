from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    print(f"Running input #{my_input} on a remote computer in the cloud!")

    sleep(1)

    return my_input


my_inputs = list(range(1000))

return_values = remote_parallel_map(my_function, my_inputs)

print(return_values)
