from burla import remote_parallel_map
from time import time


def my_function(my_input):

    called_at = time()
    return my_input


inputs = list(range(1000))

start = time()
# print(f"STARTED AT: {start}")
result_generator = remote_parallel_map(my_function, inputs, spinner=False)

# print("[")
results = []
# result_received_at = []
for result in result_generator:
    # result_received_at.append(time())
    # print(f"{time()},")
    results.append(result)

# print("]")

# print("result_received_at times:")
# print(result_received_at)
print("--------------------------------------------------")
print(f"Done after {time() - start}s")
print(f"num results: {len(results)}")
# print("results: (document claimed at times)")
print(results)

unprocessed_inputs = [i for i in inputs if i not in results]
print(f"{len(unprocessed_inputs)} unprocessed inputs: {unprocessed_inputs}")
