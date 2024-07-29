from pathlib import Path
import subprocess


from utils import with_packages, remote_parallel_map as remote_parallel_map


@with_packages(["spacy", "datasets"])
def test_spacy():
    import spacy
    import datasets

    dataset = datasets.load_dataset("wikitext", "wikitext-2-raw-v1", split="test")
    nlp = spacy.load("en_core_web_sm")

    def my_spacy_function(sample_text):
        return nlp(sample_text)

    inputs = [sample["text"] for sample in dataset]

    # packages is explicitly defined here because the import statements are not detected above
    # since they are in a function
    docs = remote_parallel_map(my_spacy_function, inputs[:100], packages=["spacy", "datasets"])

    n_tokens = sum(len(d) for d in docs)
    n_stop_tokens = sum(token.is_stop for d in docs for token in d)
    print(f"Out of the {len(docs)} sentences, {n_stop_tokens} / {n_tokens} were stop tokens")


@with_packages(["tensorflow"])
def test_gpu():
    from tensorflow.config import list_physical_devices

    def my_function(my_input):
        print(list_physical_devices("GPU"))

    my_inputs = list(range(1))

    remote_parallel_map(my_function, my_inputs, func_gpu=1)


@with_packages(["pandas"])
def test_pandas():
    import pandas as pd

    def my_function(my_input):
        data = {}

        for i in range(1000):
            data[f"thing_{i}"] = ["a"] * 1000

        return pd.DataFrame(data)

    my_inputs = list(range(100))

    outputs = remote_parallel_map(my_function, my_inputs, packages=["pandas"])

    print(outputs[0])


def test_nas():
    test_file = Path("thing.txt")
    test_file.touch()
    test_file.write_text("hello")

    try:
        subprocess.run(
            ["burla", "nas", "upload", "thing.txt"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise Exception(e.stderr)

    def my_function(my_input):
        assert Path("thing.txt").read_text() == "hello"

        test_file_2 = Path("thing2.txt")
        test_file_2.touch()
        test_file_2.write_text("world")

    my_inputs = list(range(1))

    # not sure why pytest is needed but it failed without it
    remote_parallel_map(my_function, my_inputs, packages=["pytest"])

    try:
        subprocess.run(
            ["burla", "nas", "download", "thing2.txt"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise Exception(e.stderr)

    test_file_2 = Path("thing2.txt")
    assert test_file_2.read_text() == "world"

    test_file.unlink()
    test_file_2.unlink()


def test_base():

    def my_function(my_input):
        print(my_input)
        return my_input

    something = remote_parallel_map(my_function, list(range(2)))
    print(something)


def test_trinity():
    def thing(something: str):
        import psutil

        print(psutil.virtual_memory())

        import os

        print("\n")
        print(os.cpu_count())
        print("\n")

        try:
            result = subprocess.run(
                ["Trinity", "--version"],
                capture_output=True,
                text=True,
                check=True,
            )
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            raise Exception(e.stderr)

    remote_parallel_map(
        thing,
        [1, 2, 3],
        func_cpu=96,
        func_ram=624,
        image="trinityrnaseq/trinityrnaseq",
        packages=["psutil"],
    )
