from time import sleep, time
from burla import remote_parallel_map


def my_function(my_input):

    # print(f"Running input#{my_input} it's own container in the cloud!")
    sleep(1)

    return my_input


my_inputs = list(range(200))

start = time()
results = remote_parallel_map(my_function, my_inputs)
print(f"Time taken: {time() - start}")
