from burla import remote_parallel_map


def my_function(my_input):
    # print(f"hi #{my_input}")
    return my_input * 2


inputs = list(range(5_000_000))

result_generator = remote_parallel_map(my_function, inputs, spinner=False)

results = [result for result in result_generator]

print(f"num results: {len(results)}")
# print("results: (document claimed at times)")
# print(results)

# unprocessed_inputs = [i for i in inputs if i not in results]
# print(f"{len(unprocessed_inputs)} unprocessed inputs: {unprocessed_inputs}")
