import os
from time import time, sleep

from burla import remote_parallel_map

os.environ["BURLA_API_URL"] = "http://localhost:5001"


def simple_test_function(my_input):

    print(f"processing input #{my_input}")
    sleep(1)

    return my_input


my_inputs = list(range(1000))


start = time()

list_of_return_values = remote_parallel_map(simple_test_function, my_inputs)

print(f"Time taken: {time() - start} seconds")
