<br>
<p align="center">
  <a href="https://burla.dev">
    <img src="https://backend.burla.dev/static/logo.svg" width="300">
  </a>
</p>
<br>
<p align="center">
  <img src="https://img.shields.io/pypi/v/burla?style=for-the-badge" height="24">
  <img src="https://img.shields.io/pypi/dm/burla?style=for-the-badge" height="24">
  <img src="https://img.shields.io/github/stars/Burla-Cloud/burla?style=for-the-badge&logo=github&logoColor=white" height="24">
  <img src="https://img.shields.io/badge/docs-gitbook-3C5B65?style=for-the-badge&logo=gitbook&logoColor=white&radius=20" height="24">
  <img src="https://img.shields.io/badge/python-3.10+-3C5B65?style=for-the-badge&logo=python&logoColor=white&radius=20" height="24">
</p>

## Scale Python across 1000 computers in 1 second, &nbsp;using one line of code.

Burla is a package with only **one function**. &nbsp;Here's how it works:  

``` python
from burla import remote_parallel_map

my_inputs = list(range(1000))

def my_function(x):
    print(f"I'm running on my own separate computer in the cloud! #{x}")

remote_parallel_map(my_function, my_inputs)
```
**This runs `my_function` on 1000 vm's in the cloud, in 1 second.**
<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/final_terminal_with_header_rounded.gif" alt="Burla terminal demo showing remote_parallel_map running on 1,000 computers" />
</p>
<a href="https://colab.research.google.com/drive/1msf0EWJA2wdH4QG5wPX2BncSEr5uVufv?usp=sharing">
  <img src="https://github.com/user-attachments/assets/e4665337-cb9d-4a85-8bbc-b330a3b2fb8a" />
</a>

### Scales up to 10,000 CPU's, &nbsp;with any Docker container, &nbsp;and any GPU: H100, A100 ...

Burla is simple enough for anyone to learn, yet extremely flexible and scalable.

- **Scalable:** See our demo! Processing [2.4TB in 76 seconds](https://docs.burla.dev/examples/process-2.4tb-of-parquet-files-in-76s) using 10,000 CPUs.
- **Flexible:** Easily use big VM's: `remote_parallel_map(..., func_cpu=64)`, GPU's, or custom containers.

Burla comes with a dashboard to monitor long-running workloads and manage resources:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/new_platform_demo.gif" alt="Burla dashboard demo" />
</p>

### How it works

With Burla, **running code in the cloud feels the same as coding on your laptop**:

```python
from burla import remote_parallel_map

return_values = remote_parallel_map(my_function, my_inputs)
```

When functions run with `remote_parallel_map`:

- Anything they print appears locally (and in Burla's dashboard).
- Exceptions are thrown locally.
- Packages and local modules are cloned onto remote machines.
- Code starts running in under one second, even with millions of inputs.

### Features

| Feature | Description |
| --- | --- |
| **📦 Automatic Package Sync** | Burla quickly clones your Python packages to every remote machine where your code runs. |
| **🐋 Custom Containers** | Run code in any Docker container. Public or private, paste an image URI in settings and start. |
| **📂 Network Filesystem** | Burla mounts cloud storage to `./shared` in every container for easy data exchange. |
| **⚙️ Variable Hardware Per Function** | Use `func_cpu` and `func_ram` to give different functions different hardware sizes. |

### Convert any workload into a scalable data pipeline

Have a workload that takes forever to run?

By adding `remote_parallel_map` calls, data scientists, ML engineers, and analysts can build pipelines that process terabytes of data in minutes.

The filesystem mounted at `./shared` makes it simple to process data stored in cloud storage.

```python
from burla import remote_parallel_map

# Run `process_file` on many small machines
results = remote_parallel_map(process_file, files)

# Combine results on one big machine
result = remote_parallel_map(combine_results, [results], func_cpu=64)
```

<p align="center">The example above demonstrates a basic map-reduce operation.</p>

### Examples:
- [Process 2.4TB of Parquet Files in 76s with 10,000 CPUs](https://docs.burla.dev/examples/process-2.4tb-of-parquet-files-in-76s)
- [Hyperparameter Tune XGBoost using 1,000 CPUs](https://docs.burla.dev/examples/parallel-hyperparameter-tuning)
- [Genome Alignments using 1,300 CPUs](https://docs.burla.dev/examples/multi-stage-genomic-pipeline)

Learn more at [Burla.dev](https://docs.burla.dev/)

---

Questions?  
[Schedule a call](http://cal.com/jakez/burla), or email **jake@burla.dev**. We're always happy to talk.
