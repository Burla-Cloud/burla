from burla import remote_parallel_map


def test_base():

    def my_function(my_input):
        print(my_input)
        return my_input

    something = remote_parallel_map(my_function, list(range(2)))
    print(something)
