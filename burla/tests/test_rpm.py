from time import time

from burla import remote_parallel_map


def test_base():

    def my_function(my_input):
        print(my_input)
        return my_input

    my_inputs = list(range(5))
    start = time()

    generator = remote_parallel_map(my_function, my_inputs)
    results = list(generator)

    print(f"Done after {time()-start}s")
    print(results)
