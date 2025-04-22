from burla import remote_parallel_map
from time import sleep
import os


os.environ["BURLA_API_URL"] = "http://localhost:5001"


my_inputs = list(range(1_000_000))


def simple_test_function(test_input):
    sleep(1)
    return test_input


results = remote_parallel_map(simple_test_function, my_inputs)

# print(results)
