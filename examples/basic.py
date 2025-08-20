from time import sleep
from burla import remote_parallel_map

my_inputs = list(range(1000))


def my_function(my_input):
    print(f"my input: {my_input}")
    # for x in range(100):
    #     # sleep(1)
    #     print(f"hi #{x}")


remote_parallel_map(my_function, my_inputs)
remote_parallel_map(my_function, my_inputs)
