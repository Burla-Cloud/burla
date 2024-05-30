<p align="center"><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/main/media/readme_banner.png" width=1000></p>


### Burla is currently under devlopment and is not ready to be used.

To join our mailing list go to [burla.dev](https://burla.dev/).  
If you have any questions, email me at: jake@burla.dev, or [join us on Discord](https://discord.gg/TsbCUwBUdy).

#### Current todo:
(not in any particular order)

- Switch communication between containers and clients from:  
`container -> firebase -> main-service -> client` to `container -> pub/sub -> client`  
Both for:
  - Sending function inputs from client to containers and
  - Sending stdout/err from containers to client
- Eliminate the container service.
Node service should interact only with container stdin/out/err, we shouldn't be running a custom webservice inside each container.
- Ability to start some containers on-demand while leaving others running at all times  
(currently all containers are left running at all times)
- Ability to cache python environments instead of building every time.
- Investigate faster container runtimes

### Overview:

#### Burla is a python package that makes it easy to run code on (lots of) other computers.

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

### Components / How it works:

Unlike many open-source projects Burla does not to use a monorepo.  
Instead major components are split across 4 separate GitHub repositories:

1. [Burla](https://github.com/burla-cloud/burla)  
   The python package (the client) (this repository).
2. [main_service](https://github.com/burla-cloud/main_service)  
   Service representing a single cluster, manages nodes, routes requests to node_services.
3. [node_service](https://github.com/burla-cloud/node_service)  
   Service running on each node, manages containers, routes requests to container_services.
4. [container_service](https://github.com/burla-cloud/container_service)  
   Service running inside each container, executes user submitted functions.

Read about how Burla works: [How-Burla-works.md]("https://docs.burla.dev")
