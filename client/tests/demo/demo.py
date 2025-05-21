from time import sleep
from burla import remote_parallel_map


def my_function(my_input):

    print("hi")

    sleep(1)


my_inputs = list(range(1000000))


print("starting")

remote_parallel_map(my_function, my_inputs, background=True)
