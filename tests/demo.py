from burla import remote_parallel_map


def my_function(my_input):
    print(my_input)
    return my_input


inputs = [1, 2, 3, 4]

results = remote_parallel_map(my_function, inputs)

print(results)
