from burla import remote_parallel_map


def my_function(my_input):
    print("I'm running on a remote computer in the cloud!")


remote_parallel_map(my_function, list(range(10001)))
