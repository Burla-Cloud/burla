import os
from time import sleep

from burla import remote_parallel_map

os.environ["BURLA_API_URL"] = "http://localhost:5001"


def test_function(x):

    print("hi")

    return x * 2


my_inputs = list(range(1000))
results = remote_parallel_map(test_function, my_inputs, func_cpu=96)
print(results)
