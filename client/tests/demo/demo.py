from burla import remote_parallel_map

my_inputs = list(range(1000))

def my_function(my_input):
    # (do something intense here)
    print(f"Running input #{my_input} on a separate computer in the cloud!")


remote_parallel_map(my_function, my_inputs)




