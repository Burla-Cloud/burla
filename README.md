<p align="center"><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/main/media/readme_banner.png" width=1000></p>

# Burla

Burla is a python package that makes it easy to run code on (lots of) other computers.

Burla only has one function: `remote_parallel_map`.  
This function requires just two arguments, here's how it works:

```python
from burla import remote_parallel_map

# Arg 1: Any python function:
def my_function(my_input):
    ...

# Arg 2: List of inputs for `my_function`
my_inputs = [1, 2, 3, ...]

# Calls `my_function` on every input in `my_inputs`,
# at the same time, each on a separate computer in the cloud.
remote_parallel_map(my_function, my_inputs)
```

- Burla is **fast** and **scalable**.  
  Code starts running within <u>1 second</u>, on up to <u>1000 CPU's</u>.
- Running code remotely with Burla **feels like local development**. This means that:
  - Errors thrown on remote computers are raised on your local machine.
  - Anything you print appears in the terminal on your local machine.
  - Your python environment is automaticaly cloned on all remote computers.  
    This allows you to call any local python package in a function sent to `remote_parallel_map`.  
    After installing once, environments are cached to keep latency below 1 second.
- Burla is **easy to install**.  
  Try our managed service with [two commands](https://docs.burla.dev/Getting-Started#getting-started-fully-managed). Install Burla in your cloud with [three commands](https://docs.burla.dev/Getting-Started#getting-started-self-managed-gcp-only).
- Burla supports **custom resource requirements**.  
  Allocate up to 96 CPUs and 360G of ram to each individual function call with [two simple arguments](https://docs.burla.dev/API-Reference).
- Burla **supports GPU's**.  
  Just add one argument: `remote_parallel_map(my_function, my_inputs, gpu="A100")`
- Burla supports **custom Docker images**.  
  Just add one argument: `remote_parallel_map(my_function, my_inputs, dockerfile="./Dockerfile")`  
  After building once, images are cached to keep latency below 1 second.
- Burla offers **simple network storage**.  
  By default, all remote machines are attached to the same persistent network disk.  
  Upload & download files to this disk through a simple CLI: `> burla nas upload / download / ls / rm ...`

#### Burla is currently under devlopment and is not ready to be used.

To join our mailing list go to [burla.dev](https://burla.dev/).  
If you have any questions, email me at: [jake@burla.dev](mailto:jake@burla.dev), or [join us on Discord](https://discord.gg/xSuJukdS9b).
