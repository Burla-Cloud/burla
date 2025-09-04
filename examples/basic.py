from burla import remote_parallel_map


def my_function(x):
    print(f"Running on a remote computer in the cloud! #{x}")


remote_parallel_map(my_function, list(range(1000)))
