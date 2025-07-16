from burla import remote_parallel_map


def my_function(my_input):

    print(f"I'm running input on a remote computer in the cloud!  #{my_input}")


remote_parallel_map(my_function, list(range(1000)))


pass
