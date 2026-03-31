## Scale Python across 1000 computers in 1 second.

Burla is a Python package with only **one function**:

```py
from burla import remote_parallel_map

my_inputs = list(range(1000))

def my_function(x):
    print(f"[#{x}] running on separate computer")

remote_parallel_map(my_function, my_inputs)
```

**This runs `my_function` on 1000 VMs in the cloud in under one second.**

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/final_terminal.gif" alt="Burla terminal demo showing remote_parallel_map running on 1,000 computers" />
</p>

## Burla is the simplest way to scale any data pipeline.

Burla scales up to 10,000 CPUs in a single function call and supports GPUs plus custom containers.
Load data in parallel from cloud storage, then write results in parallel from thousands of VMs at once.

```py
remote_parallel_map(process, [...])
remote_parallel_map(aggregate, [...], func_cpu=64)
remote_parallel_map(predict, [...], func_gpu="A100")
```

This creates a pipeline like:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/output-onlinegiftools%20%281%29%20%281%29.gif" alt="Burla data pipeline diagram animation" />
</p>

Burla includes a dashboard so you can monitor progress, cancel bad runs, and inspect logs.

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/area2-rounded-white-r60-exact-size.gif" alt="Burla dashboard demo" />
</p>

## How it works

With Burla, running code in the cloud feels the same as coding on your laptop:

```py
return_values = remote_parallel_map(my_function, my_inputs)
```

When functions run with `remote_parallel_map`:

- Anything they print appears locally (and in Burla's dashboard).
- Exceptions are thrown locally.
- Packages and local modules are cloned onto remote machines.
- Code starts running in under one second, even with millions of inputs.

## Features

- **Automatic Package Sync**  
  Burla quickly clones your Python packages to every remote machine where your code runs.

- **Custom Containers**  
  Run code in any Docker container, public or private.

- **Network Filesystem**  
  Burla mounts cloud storage to `./shared` in every container for easy data exchange.

- **Variable Hardware Per Function**  
  Use `func_cpu` and `func_ram` to give different functions different hardware sizes.

## Try Burla in less than 2 minutes

1. Sign in at [login.burla.dev](https://login.burla.dev/).
2. Follow the 3-step quickstart on the Burla homepage.

Burla is open-source and easy to self-host. See the self-hosted quickstart:
[docs.burla.dev/get-started#quickstart-self-hosted](https://docs.burla.dev/get-started#quickstart-self-hosted)

## Examples

- [Process 2.4TB of Parquet files in 76s with 10,000 CPUs](https://docs.burla.dev/examples/process-2.4tb-of-parquet-files-in-76s)
- [Hyperparameter tune XGBoost using 1,000 CPUs](https://docs.burla.dev/examples/parallel-hyperparameter-tuning)
- [Genome alignments using 1,300 CPUs](https://docs.burla.dev/examples/multi-stage-genomic-pipeline)

Learn more at [docs.burla.dev](https://docs.burla.dev/).

---

Questions?  
[Schedule a call](http://cal.com/jakez/burla), or email **jake@burla.dev**.
