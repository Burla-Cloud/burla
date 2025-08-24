from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(1000))


def my_function3(my_input):
    sleep(1)
    print(f"Running Input #{my_input} on it's own separate computer in the cloud!")
    if my_input == 10:
        raise Exception("This is a test error")


# remote_parallel_map(my_function2, my_inputs)
remote_parallel_map(my_function3, my_inputs, background=True)
