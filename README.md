<br>
<p align="center">
  <a href="https://burla.dev">
    <img src="https://backend.burla.dev/static/logo.svg" width="264">
  </a>
</p>
<br>
<p align="center">
  <a href="https://pypi.org/project/burla/"><img src="https://img.shields.io/pypi/v/burla?style=for-the-badge" height="22"></a>
  <a href="https://pepy.tech/projects/burla"><img src="https://img.shields.io/pepy/dt/burla?style=for-the-badge&color=brightgreen" height="22"></a>
  <a href="https://github.com/Burla-Cloud/burla/stargazers"><img src="https://img.shields.io/github/stars/Burla-Cloud/burla?style=for-the-badge&logo=github&logoColor=white" height="22"></a>
  <a href="https://github.com/Burla-Cloud/burla/commits/main"><img src="https://img.shields.io/github/last-commit/Burla-Cloud/burla?style=for-the-badge&color=brightgreen" height="22"></a>
  <a href="https://docs.burla.dev"><img src="https://img.shields.io/badge/docs-gitbook-3C5B65?style=for-the-badge&logo=gitbook&logoColor=white&radius=20" height="22"></a>
</p>

## Scale Python across 1,000 computers in 1 second.

Burla is an open-source cloud platform for Python developers. It only has one function:

```py
from burla import remote_parallel_map

my_inputs = list(range(1000))

def my_function(x):
    print(f"[#{x}] running on separate computer")

remote_parallel_map(my_function, my_inputs)
```

This runs `my_function` on 1,000 VMs in the cloud, in < 1 second:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/hell_cut_extended_no-zsh.gif" alt="Burla terminal demo showing remote_parallel_map running on 1,000 computers" width="90%" />
</p>

## The simplest way to build scalable data-pipelines.

Burla scales up to 10,000 CPUs in a single function call, supports GPUs, and custom containers.  
Load data in parallel from cloud storage, then write results in parallel from thousands of VMs at once.

```py
remote_parallel_map(process, [...])
remote_parallel_map(aggregate, [...], func_cpu=64)
remote_parallel_map(predict, [...], func_gpu="A100")
```

This creates a pipeline like:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/output-onlinegiftools%20%281%29%20%281%29.gif" alt="Burla data pipeline animation" width="80%" />
</p>

### Monitor progress in the dashboard:

Cancel bad runs, filter logs to watch individual inputs, or monitor output files in the UI.

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/area2-radius60-247-251-252.gif" alt="Burla dashboard demo" />
</p>

## How it works:

With Burla, **running code in the cloud feels the same as coding on your laptop:**

```py
return_values = remote_parallel_map(my_function, my_inputs)
```

When functions are run with `remote_parallel_map`:

- Anything they print appears locally (and inside Burla's dashboard).
- Any exceptions are thrown locally.
- Any packages or local modules they use are (very quickly) cloned on remote machines.
- Code starts running in under one second, even with millions of inputs or thousands of machines.

### Features:

- **📦  Automatic Package Sync**  
  Burla automatically (and very quickly) clones your Python packages on every remote machine where code is executed.

- **🐋  Custom Containers**  
  Easily run code in any Docker container. Public or private, just paste an image URI in the settings, then hit start.

- **📂  Network Filesystem**  
  Need to get big data into or out of the cluster? Burla automatically mounts a cloud storage bucket to a folder in every container.

- **⚙️  Variable Hardware Per-Function**  
  The `func_cpu` and `func_ram` args make it possible to assign big hardware to some functions, and less to others.

### Try Burla for Free, using 1,000 CPUs!

1. [Sign in](https://login.burla.dev/) using your Google or Microsoft account.
2. Run the quickstart in this Google Colab notebook (takes less than 2 minutes):

<a href="https://colab.research.google.com/drive/1msf0EWJA2wdH4QG5wPX2BncSEr5uVufv?usp=sharing">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/.github/main/assets/colab-button.png" alt="Open Burla quickstart in Google Colab" />
</a>

### Examples

- [Process 2.4TB of Parquet files in 76s with 10,000 CPUs](https://docs.burla.dev/examples/process-2.4tb-of-parquet-files-in-76s)
- [Hyperparameter tune XGBoost using 1,000 CPUs](https://docs.burla.dev/examples/parallel-hyperparameter-tuning)
- [Genome alignments using 1,300 CPUs](https://docs.burla.dev/examples/multi-stage-genomic-pipeline)

Learn more at [docs.burla.dev](https://docs.burla.dev/).
