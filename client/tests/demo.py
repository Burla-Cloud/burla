import os
from time import sleep

from burla import remote_parallel_map

os.environ["BURLA_API_URL"] = "http://localhost:5001"


def test_function(x):

    print("hi")
    sleep(0.1)

    # return x


my_inputs = list(range(1_000))
remote_parallel_map(test_function, my_inputs, background=True)
