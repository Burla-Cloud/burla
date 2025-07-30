from burla import remote_parallel_map

my_inputs = list(range(10))


def my_function(my_input):
    print(f"I'm running on my own separate computer in the cloud! #{my_input}")


remote_parallel_map(my_function, my_inputs)
