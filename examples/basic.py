from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(1))


def my_function6(my_input):
    # sleep(1)
    # if my_input == 200:
    #     raise Exception("This is a test error")

    for i in range(100):
        print(f"Running Input #{my_input} on it's own separate computer in the cloud! {i}")
        sleep(1)


# remote_parallel_map(my_function6, my_inputs)
remote_parallel_map(my_function6, my_inputs, background=True)
