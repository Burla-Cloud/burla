"""
The tests here assume the cluster is running in "local-dev-mode".
"""

from time import sleep
from burla import remote_parallel_map


N_INPUTS = 160


def test_base():

    def test_function(test_input):
        return test_input

    results = remote_parallel_map(test_function, list(range(N_INPUTS)))


def _test_big_function():

    big_object = bytes(107 * 1_000_000)

    def test_function(test_input):
        return test_input

    results = remote_parallel_map(test_function, list(range(N_INPUTS)))


def _test_big_inputs():

    INPUT_SIZE = 10 * 1_000_000
    my_inputs = [bytes(INPUT_SIZE) for _ in range(1_000)]

    def test_function(test_input):
        sleep(1)
        return 1

    results = remote_parallel_map(test_function, my_inputs)


def _test_big_returns():

    RETURN_SIZE = 100 * 1_000_000

    def test_function(test_input):
        return bytes(RETURN_SIZE)

    results = remote_parallel_map(test_function, list(range(100)))
