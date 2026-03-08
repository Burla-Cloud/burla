## Scale Python across 1,000s of computers using one line of code.

Burla is a Python package with **one function**: `remote_parallel_map`. Here's an example:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/CleanShot%202026-01-18%20at%2015.07.24.png" alt="Burla code example" />
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/final_terminal_with_header_rounded.gif" alt="Burla terminal demo showing remote_parallel_map running on 1,000 computers" />
</p>

<p align="center">This realtime example runs <code>my_function</code> on 1,000 separate computers in one second.</p>

### Enable anyone to process terabytes of data in minutes, not days.

Burla is simple enough for anyone to learn, yet extremely scalable and flexible.

- **Scalable:** See our [2.4TB in 76 seconds demo](https://docs.burla.dev/examples/process-2.4tb-of-parquet-files-in-76s), where Burla uses 10,000 CPUs.
- **Flexible:** Run any code inside any Docker container on any hardware, including GPUs and TPUs.

Easily monitor long-running workloads and manage compute resources in the dashboard.

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

By adding `remote_parallel_map` calls, data scientists, ML engineers, and analysts can process terabytes of data and finish in minutes.

The filesystem mounted at `./shared` makes it simple to process data stored in cloud storage.

```python
from burla import remote_parallel_map

# Run `process_file` on many small machines
results = remote_parallel_map(process_file, files)

# Combine results on one big machine
result = remote_parallel_map(combine_results, [results], func_cpu=64)
```

<p align="center">The example above demonstrates a basic map-reduce operation.</p>

### Burla only takes 2 minutes to try

[![Try Burla for free](https://img.shields.io/badge/Try%20Burla%20for%20free-111827?style=for-the-badge)](https://login.burla.dev/)

1. Sign in with your Google or Microsoft account.
2. Click `⏻ Start` to boot some computers.
3. Scale Python over 1,000 CPUs in [this Google Colab notebook](https://colab.research.google.com/drive/1bR8Gpa85gqJi7_9uKdcJDX9_WG0tuVmG?usp=sharing).

Quick reminder: Burla is open source and easy to self-host. [Deploy Burla in your cloud](https://docs.burla.dev/get-started#quickstart-self-hosted).

---

Questions?  
[Schedule a call](http://cal.com/jakez/burla), or email **jake@burla.dev**. We're always happy to talk.
