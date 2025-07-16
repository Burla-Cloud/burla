from burla import remote_parallel_map


def my_function(my_input):

    # Something intense goes here!

    return my_input * 2


return_values = remote_parallel_map(my_function, list(range(10_000_000)))
print(return_values)


pass
