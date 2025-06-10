import time

# from time import sleep, time
import burla

cache = {}


def my_function(my_input):

    if cache.get("test"):
        print(f"cache value is: {cache['test']}")
    else:
        print("setting cache!!!")
        cache["test"] = "hi"

    # print(f"Running input#{my_input} it's own container in the cloud!")
    # sleep(1)

    return my_input


my_inputs = list(range(200))

# start = time()
results = burla.remote_parallel_map(my_function, my_inputs)
# print(f"Time taken: {time() - start}")
