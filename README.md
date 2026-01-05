
### Run any Python function on 1000 computers in 1 second.

Burla makes it trivial to run Python functions on thousands of computers in the cloud.\
It's a package that only has **one function**:

<figure><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/refs/heads/main/media/main_demo.gif" alt="" style="width:80%" /><figcaption></figcaption></figure>

This realtime example runs <code>my_function</code> on 1,000 separate computers in one second!

### The full power of the cloud, in an easy to use, open platform:

Burla enables anyone, even total beginners, to harness the full power of the cloud:

* **Scalability:** See our [demo](examples/process-2.4tb-of-parquet-files-in-76s.md) where we process 2.4TB in 76s using 10,000 CPUs.
* **Flexibility:** Runs any Python function, inside any Docker container, on any hardware.
* **Simplicity:** Burla is just one function, with two required arguments.

Easily monitor workloads, and manage infrastructure from our open-source web dashboard:

<figure><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/refs/heads/main/media/platform_demo.gif" alt="" style="width:80%" /><figcaption></figcaption></figure>

### **How it works:**

Burla only has one function: `remote_parallel_map`  \
When called, it runs the given function, on every input in the given list, each on a separate computer.

```python
from burla import remote_parallel_map

my_inputs = [1, 2, 3]

def my_function(my_input):
    print("I'm running on my own separate computer in the cloud!")
    return my_input
    
return_values = remote_parallel_map(my_function, my_inputs)
```

Running code in the cloud with Burla feels the same as coding locally:

* Anything you print appears in your local terminal.
* Exceptions thrown in your code are thrown on your local machine.
* Responses are quick, you run a million function calls in a couple seconds!

### Features:

#### 📦 Automatic Package Sync

Burla clusters automatically (and very quickly) install any missing python packages into all containers in the cluster.

#### 🐋 Custom Containers

Easily run code in any Docker container. Public or private, just paste an image URI in the settings, then hit start!

#### 📂 Network Filesystem

Need to get big data into/out of the cluster? Burla automatically mounts a cloud storage bucket to `./shared` in every container.

#### ⚙️ Variable Hardware Per-Function

The `func_cpu` and `func_ram` args make it possible to assign more hardware to some functions, and less to others, unlocking new ways to simplify pipelines and architecture.


### Build scalable data-pipelines using plain Python:

Fan code across thousands of machines, then combine results on one big machine.\
The network filesystem mounted at `./shared` makes it easy to pass big data between steps.

```python
from burla import remote_parallel_map

# Run `process_file` on many small machines
results = remote_parallel_map(process_file, files)

# Combine results on one big machine
result = remote_parallel_map(combine_results, [results], func_ram=256)
```

The above example demonstrates a basic map-reduce operation.

### Demo:

[https://www.youtube.com/watch?v=9d22y_kWjyE](https://www.youtube.com/watch?v=9d22y_kWjyE)

### Try it out today:

There are two ways to host Burla:

1. **In your cloud.**\
   Burla is open-source, and can be deployed with one command (currently Google-Cloud only).\
   [Click here](https://docs.burla.dev/get-started#quickstart-self-hosted) to get started with self-hosted Burla.
2. **In our cloud.**\
   First $1,000 in compute spend is free, try it now: [https://burla.dev/signup](https://docs.burla.dev/signup)

***

Questions?\
[Schedule a call](http://cal.com/jakez/burla), or email **jake@burla.dev**. We're always happy to talk.
