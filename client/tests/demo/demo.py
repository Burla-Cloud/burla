from burla import remote_parallel_map


def my_function(my_input):
    # Something intense goes here!
    pass


remote_parallel_map(my_function, list(range(10_000_000)))


pass
