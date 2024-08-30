from burla import remote_parallel_map


def my_function(my_input):
    print(f"hi #{my_input}")
    return my_input * 2


inputs = list(range(8))

results = remote_parallel_map(my_function, inputs)

print(f"num results: {len(results)}")
print(results)
