from time import sleep, time
import burla


# def my_function(my_input):

#     sleep(0.1)

#     return my_input


# my_inputs = list(range(100))

# start = time()
# results = burla.remote_parallel_map(my_function, my_inputs)
# print(f"Time taken: {time() - start}")


import subprocess

worker_cache = {}


def do_inference(prompt: str):
    result = subprocess.run(["nvidia-smi"], check=True, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # if not worker_cache.get("llm"):
    #     print("Loading LLM onto GPU")
    #     worker_cache["llm"] = LLM(model="meta-llama/Llama-4-Scout-17B-16E-Instruct")
    # else:
    #     print("Using cached LLM")

    # print(f"Asking LLM: {prompt}")
    # result = worker_cache["llm"].generate(prompt)
    # response = result[0].outputs[0].text
    # print(f"Response: {response}\n\n")

    return prompt


prompts = [
    "Explain quantum computing in simple terms.",
    "Summarize the plot of Inception.",
    "What causes inflation in an economy?",
    "Write a haiku about spring.",
    "Describe how photosynthesis works.",
    "Give a recipe for chocolate chip cookies.",
    "What are black holes?",
    "Translate 'Good morning' into French.",
    "Explain the theory of relativity.",
    "Write a tweet about AI safety.",
]

results = burla.remote_parallel_map(do_inference, [prompts[0]])

print(results)
