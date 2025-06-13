from time import sleep, time
import burla


def my_function(my_input):

    sleep(0.1)

    return my_input


my_inputs = list(range(100))

start = time()
results = burla.remote_parallel_map(my_function, my_inputs)
print(f"Time taken: {time() - start}")

# import os
# import subprocess

# worker_cache = {}


# def do_inference(prompt: str):
#     from vllm import LLM, SamplingParams

#     # from huggingface_hub import login

#     # login("XXXX")

#     if not worker_cache.get("llm"):
#         print("Loading LLM onto GPU")
#         worker_cache["llm"] = LLM(
#             model="meta-llama/Meta-Llama-3-8B-Instruct", dtype="float16", tensor_parallel_size=1
#         )
#     else:
#         print("Using cached LLM")

#     sampling_params = SamplingParams(temperature=0.7, top_p=0.95)
#     output = worker_cache["llm"].generate(prompt, sampling_params)
#     return output[0].outputs[0].text


# prompts = [
#     "Explain quantum computing in simple terms.",
#     "Summarize the plot of Inception.",
#     "What causes inflation in an economy?",
#     "Write a haiku about spring.",
#     "Describe how photosynthesis works.",
#     "Give a recipe for chocolate chip cookies.",
#     "What are black holes?",
#     "Translate 'Good morning' into French.",
#     "Explain the theory of relativity.",
#     "Write a tweet about AI safety.",
# ]

# results = burla.remote_parallel_map(do_inference, prompts)

# print(results)
