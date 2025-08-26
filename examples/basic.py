from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(1000))


def my_function5(my_input):
    # sleep(1)
    # if my_input == 20:
    #     raise Exception("This is a test error")
    print(f"Running Input #{my_input} on it's own separate computer in the cloud!")


# remote_parallel_map(my_function2, my_inputs)
remote_parallel_map(my_function5, my_inputs)
