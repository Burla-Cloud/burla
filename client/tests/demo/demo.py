from time import sleep, time
import burla


# def my_function(my_input):

#     sleep(1)

#     return my_input


# my_inputs = list(range(200))

# # start = time()
# results = burla.remote_parallel_map(my_function, my_inputs)
# # print(f"Time taken: {time() - start}")


from vllm import LLM


def do_inference(prompt: str):
    llm = LLM(model="meta-llama/Llama-4-Scout-17B-16E-Instruct")
    response = llm.generate(prompt)
    return response[0].outputs[0].text


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

results = burla.remote_parallel_map(do_inference, prompts)

print(results)
