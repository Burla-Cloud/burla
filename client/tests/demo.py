import os
from time import sleep

from burla import remote_parallel_map

os.environ["BURLA_API_URL"] = "http://localhost:5001"


def test_function(x):

    sleep(0.2)


my_inputs = list(range(1_000_000))
remote_parallel_map(test_function, my_inputs)
