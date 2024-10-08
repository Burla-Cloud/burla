from time import time

from burla import remote_parallel_map
from utils import with_packages


@with_packages(["tensorflow"])
def test_gpu():
    from tensorflow.config import list_physical_devices

    def my_function(my_input):
        print(list_physical_devices("GPU"))

    my_inputs = list(range(1))

    remote_parallel_map(my_function, my_inputs, func_gpu=1)


@with_packages(["pandas"])
def test_pandas():
    import pandas as pd

    def my_function(my_input):
        data = {}

        for i in range(1000):
            data[f"thing_{i}"] = ["a"] * 1000

        return pd.DataFrame(data)

    my_inputs = list(range(100))

    outputs = remote_parallel_map(my_function, my_inputs, packages=["pandas"])

    print(outputs[0])


def test_base():

    def my_function(my_input):
        print(my_input)
        return my_input

    my_inputs = list(range(4))
    start = time()

    generator = remote_parallel_map(my_function, my_inputs)
    results = list(generator)

    print(f"Done after {time()-start}s")
    print(results)
