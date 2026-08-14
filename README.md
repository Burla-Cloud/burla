<p align="center">
  <a href="https://burla.dev">
    <img src="https://backend.burla.dev/static/logo.svg" width="264" alt="Burla">
  </a>
</p>

<p align="center">
  <b>The simplest way to scale Python.</b>
</p>

<p align="center">
  <a href="https://burla.dev/docs">Documentation</a> ·
  <a href="https://burla.dev/docs/get-started">Getting started</a> ·
  <a href="https://burla.dev/docs/api-reference">API reference</a> ·
  <a href="https://burla.dev/docs/examples">Examples</a> ·
  <a href="https://burla.dev">Website</a>
</p>

<p align="center">
  <a href="https://pypi.org/project/burla/"><img src="https://img.shields.io/pypi/v/burla" alt="PyPI"></a>
  <a href="https://pepy.tech/projects/burla"><img src="https://img.shields.io/pepy/dt/burla?color=brightgreen" alt="Downloads"></a>
  <img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python 3.11+">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-FSL--1.1--Apache--2.0-lightgrey" alt="License"></a>
</p>

---

Burla is a distributed computing framework that runs plain Python functions across thousands of CPUs or GPUs in your own cloud account. It has exactly one function:

```python
from burla import remote_parallel_map

my_inputs = list(range(1000))

def my_function(x):
    print(f"[#{x}] running on separate computer")

remote_parallel_map(my_function, my_inputs, grow=True)
```

This example asks Burla to scale the job to 1,000 CPUs and run 1,000 function calls in parallel:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/hell_cut_extended_no-zsh.gif" alt="Burla terminal demo showing remote_parallel_map running 1,000 function calls" width="90%">
</p>

## Highlights

- **One function.** `results = remote_parallel_map(my_function, my_inputs)` is the entire API. No DAGs, no YAML, no cluster SDK to learn.
- **Feels local.** Anything your function prints streams back to your terminal. Exceptions are re-raised locally with full tracebacks. Packages missing from the image are installed automatically, and import-time local modules ship with your function.
- **Fast dispatch.** On a warm cluster, a print-only job across 1,000 CPUs completes in under a second.
- **Runs in your cloud.** Burla runs your functions on raw VMs in your own cloud account, not shared Burla infrastructure.
- **Hardware and images in code.** Request CPUs or RAM per function call, add A100 or H100 GPUs on AWS or Google Cloud, and select a compatible `linux/amd64` image.
- **Adaptive concurrency.** On CPU nodes, the default dynamic CPU and RAM settings start one worker per CPU, then reduce node concurrency under pressure when possible.
- **Built-in dashboard.** View live logs and node status locally; deploy it for background jobs and access from any device.

## Getting started

You'll need Python 3.11+, permission to boot VMs, and the CLI for AWS, Google
Cloud, or Azure installed and signed in.

```bash
pip install burla
```

If exactly one cloud CLI is installed, Burla selects it automatically. If
several are installed, an interactive command asks you to choose once and saves
the answer. In a notebook or script, select it explicitly:

```bash
burla config set cloud <aws|gcp|azure>
```

For AWS, Burla uses the account and region selected by the AWS CLI:

```bash
aws sso login  # or: aws configure
```

For Google Cloud, Burla uses the active gcloud project:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <project-id>
```

For Azure, Burla uses the active Azure subscription:

```bash
az login
az account set --subscription <subscription-id>
```

Open the dashboard. In Settings, choose a Docker image whose Python minor version
matches your local interpreter, then press **Start** to boot the first node:

```bash
burla dashboard
```

AWS uses the default VPC. Set `AWS_SUBNET_ID` and `AWS_SECURITY_GROUP_ID` only
when you need to choose existing network resources. Google Cloud uses the
default network. Azure uses an existing outbound-capable subnet; set
`AZURE_SUBNET_ID` to choose a specific one.

```python
from burla import remote_parallel_map

def my_function(x):
    print(f"processing input {x} on a machine in the cloud")
    return x * 2

results = remote_parallel_map(my_function, list(range(100)))
```

Burla runs the cluster coordinator on your machine by default.
`remote_parallel_map` starts it automatically when needed, and
`burla dashboard` reuses it. Configured nodes default to a 10-minute idle
timeout; nodes added by `grow=True` use one minute. Client-hosted Azure nodes
receive a short-lived deletion token because guest poweroff does not stop Azure
billing.

**Deploying for a team (optional):** `burla deploy` moves the coordinator and dashboard onto a small always-on VM so teammates can share one cluster. On AWS and Google Cloud, the first deploy also moves the job history and settings from your machine's coordinator. This is the only step that requires elevated cloud permissions; the exact list is in the [CLI reference](https://burla.dev/docs/cli-reference). Add teammates in the deployed dashboard's authorized-users settings; they then connect with `burla login`.

See the [getting started guide](https://burla.dev/docs/get-started) for a full walkthrough.

## Usage

Hardware and environment are arguments, not configuration:

```python
results = remote_parallel_map(
    my_function,
    my_inputs,
    func_cpu=4,           # CPUs reserved per call ("dynamic" by default)
    func_ram=16,          # GB of RAM per call ("dynamic" by default)
    func_gpu="A100",      # one GPU per call (AWS or Google Cloud)
    image="python:3.12",  # linux/amd64; match the client's Python minor version
    grow=True,            # add VMs when capacity falls short (False by default)
    generator=True,       # yield results as they finish
)
```

Custom images must expose the matching interpreter as `python` and provide
`sh`, `awk`, and `sleep`.

Because each call can use different machine sizes, types, and environments, a multi-stage pipeline over a 100+ TB dataset is just a few lines:

```python
remote_parallel_map(process, [...], image="python:3.12", grow=True)
remote_parallel_map(aggregate, [...], func_cpu=64, grow=True)
remote_parallel_map(predict, [...], func_gpu="A100", grow=True)
```

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/image%20(19).png" alt="Burla data pipeline: cloud storage to CPUs, into a 64-CPU aggregation step, out to GPUs, and back to cloud storage" width="90%">
</p>

## Efficiency

On CPU nodes with dynamic CPU and RAM, Burla starts with one worker per CPU.
When a node sees sustained CPU pressure, Burla reduces concurrency. If a worker
runs out of memory, Burla requeues its input and reduces concurrency when
possible so the remaining workers get more resources.

[Read the blog post](https://burla.dev/blog) on how adaptive concurrency works.

## Monitoring

A deployed cluster includes a dashboard with live logs, shared output files, node status, and background jobs:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/area2-radius60-247-251-252.gif" alt="Burla dashboard showing live logs, output files, and cluster status">
</p>

## Examples

<table>
  <tr>
    <td width="33%" align="center"><a href="https://burla.dev/docs/featured-examples/process-2.4tb-of-parquet-files-in-76s"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/query-2-4tb-parquet-card.png" width="100%" alt="Query 2.4TB of Parquet in 76s"><br><b>Query 2.4TB of Parquet in 76s</b></a></td>
    <td width="33%" align="center"><a href="https://burla.dev/docs/featured-examples/airbnb-burla"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/airbnb-burla-card.png" width="100%" alt="Rank 1.7M Airbnbs"><br><b>Rank 1.7M Airbnbs</b></a></td>
    <td width="33%" align="center"><a href="https://burla.dev/docs/featured-examples/amazon-review-distiller"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/amazon-review-distiller-card.png" width="100%" alt="Distill 572M Amazon reviews"><br><b>Distill 572M Amazon reviews</b></a></td>
  </tr>
  <tr>
    <td width="33%" align="center"><a href="https://burla.dev/docs/featured-examples/arxiv-fossils"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/arxiv-fossils-card.png" width="100%" alt="Cluster 2.7M arXiv abstracts"><br><b>Cluster 2.7M arXiv abstracts</b></a></td>
    <td width="33%" align="center"><a href="https://burla.dev/docs/featured-examples/multi-stage-genomic-pipeline"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/multi-stage-genomic-pipeline-card.png" width="100%" alt="Genomic alignment pipeline"><br><b>Genomic alignment pipeline</b></a></td>
    <td width="33%" align="center"><a href="https://burla.dev/docs/all-examples/data-processing-examples/world-photo-index"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/world-photo-index-card.png" width="100%" alt="Map 9.5M geotagged photos"><br><b>Map 9.5M geotagged photos</b></a></td>
  </tr>
</table>

<p align="center"><a href="https://burla.dev/docs/examples"><b>Browse all &rarr;</b></a></p>

## How it works

Burla has three top-level components in this repository:

| Directory | Runs on | Purpose |
| --- | --- | --- |
| [`client/`](client) | Your machine | The `burla` PyPI package. Pickles your function, uploads inputs, streams back logs and results. |
| [`main_service/`](main_service) | Your machine, or a small always-on VM after `burla deploy` | Control plane. Boots and deletes VMs, hosts the dashboard, handles auth. |
| [`node_service/`](node_service) | Each VM | Per-node orchestrator. Queues inputs, runs your function inside workers in your Docker image. |

When you call `remote_parallel_map`, the client sends your function and inputs over TLS to the nodes instead of through the control plane. The nodes fan work out to workers, and results, logs, and exceptions stream back to your machine. Cluster state lives in memory on the control plane, and nodes rebalance queued inputs between themselves mid-job so the cluster stays busy.

## FAQ

**How is Burla different from Ray or Dask?**
Ray and Dask are general-purpose frameworks with APIs for tasks, actors, and distributed data structures. Burla deliberately covers one case, fanning a Python function out over many machines, with per-call hardware and images, adaptive concurrency, and nothing to learn beyond `remote_parallel_map`.

**Where does my code run?**
Entirely inside your own cloud account, on VMs Burla boots and deletes for you. Your client sends work to those VMs over TLS; Burla's relay carries the encrypted traffic so your account needs no public inbound firewall rules.

**Which clouds are supported?**
Google Cloud, AWS, and Microsoft Azure.

## Contributing

Bug reports and feature requests are welcome in [GitHub issues](https://github.com/Burla-Cloud/burla/issues). If you'd like to contribute code, open an issue first so we can point you in the right direction. To report a security issue, email security@burla.dev.

## License

Burla is licensed under the [Functional Source License, Version 1.1, with Apache 2.0 Future License](LICENSE) (FSL-1.1-Apache-2.0). You can use, copy, modify, and redistribute it for any purpose except a competing commercial offering, and each version becomes available under Apache 2.0 two years after its release.

---

<p align="center">
  Questions? Email <a href="mailto:jake@burla.dev">jake@burla.dev</a> or <a href="https://cal.com/jakez/burla?user=jakez">book a call</a>, we're always happy to talk.
</p>
