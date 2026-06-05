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
  <a href="https://github.com/Burla-Cloud/burla/stargazers"><img src="https://img.shields.io/github/stars/Burla-Cloud/burla?style=for-the-badge&logo=github&logoColor=white&cacheSeconds=86400" height="22"></a>
  <a href="https://github.com/Burla-Cloud/burla/commits/main"><img src="https://img.shields.io/github/last-commit/Burla-Cloud/burla?style=for-the-badge&color=brightgreen&cacheSeconds=86400" height="22"></a>
  <a href="https://docs.burla.dev"><img src="https://img.shields.io/badge/docs-gitbook-3C5B65?style=for-the-badge&logo=gitbook&logoColor=white&radius=20" height="22"></a>
</p>

# Scale Python to 1,000 VMs in your cloud in 1 second.

Burla is a self-hostable compute platform for scaling big data workloads in your cloud.\
Run analysis, inference, embeddings, and more with instant feedback, and 2-5x higher utilization.

Burla only has one function:

```py
from burla import remote_parallel_map

my_inputs = list(range(1000))

def my_function(x):
    print(f"[#{x}] running on separate computer")

remote_parallel_map(my_function, my_inputs)
```

This example runs `my_function` on 1,000 VMs in less than one second:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/hell_cut_extended_no-zsh.gif" alt="Burla terminal demo showing remote_parallel_map running on 1,000 computers" width="90%" />
</p>

# Scalable & efficient pipelines are not straightforward.

Slow deployments, VM reboots, or container rebuilds mean waiting 5-10 minutes with every change.\
Errors are vague, and configs are full of secret tradeoffs. 90% resource utilization is a pipe dream.

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/pipeline-problems.png" alt="Cryptic errors from Airflow, Ray, Dask, and AWS Batch: Broken DAG, OutOfMemoryError, KilledWorker, and INSUFFICIENT_CAPACITY" width="90%" />
</p>

# Burla simplifies scaling with adaptive infrastructure.

Easily fan Python in/out across thousands of machines with varying sizes, types, and environments.\
Quickly develop pipelines that handle 100+ TB datasets, using simple code anyone can understand.

This code:

```py
remote_parallel_map(process, [...], image="rocker/geospatial:latest")
remote_parallel_map(aggregate, [...], func_cpu=64)
remote_parallel_map(predict, [...], func_gpu="A100")
```

Creates a pipeline like:

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/image%20(19).png" alt="Burla data pipeline: cloud storage to CPUs, into a 64-CPU aggregation step, out to GPUs, and back to cloud storage" width="90%" />
</p>

Burla automatically adjusts it's own pool of VMs underneath to maximize speed and efficiency.\
Not only is this easier (no YAML, no config footguns), it's often 2-5x more compute efficient.

# Infra that manages itself is over twice as efficient.

Burla vertically scales hardware available to each function call live while the program is running.\
This frequently more than doubles compute efficiency, and eliminates out of memory errors.

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/image.png" alt="CPU demand curve rising and falling while Burla adds and removes machines to match it" width="90%" />
</p>

```py
remote_parallel_map(..., func_ram="dynamic", func_cpu="dynamic")
```

This system is possible due to Burla's unique architecture lacking a traditional master node.\
Read [our blog post](https://docs.burla.dev/blog/dynamic-hardware) to learn more about dynamic hardware.

# How it works

Running code in the cloud shouldn't feel any different from running code locally.

```py
return_values = remote_parallel_map(my_function, my_inputs)
```

When a Python function is run using `remote_parallel_map`, it runs in the cloud but:

- Anything it prints appears locally (and inside the dashboard).
- Any exceptions are thrown locally.
- Any packages or local modules are (very quickly) cloned on all remote machines.
- Code starts running in under one second! Even with millions of inputs, or thousands of machines.

Code runs on a pool of VM's that are automatically managed by Burla to maximize efficiency.\
You can manually add & remove machines from the pool, or let the platform react live to requests.

# A full platform to scale up and monitor any workload.

Keep an eye on your analysis, pipeline, or background job from your phone.\
Burla has all the features you need to closely monitor logs, output files, and available compute.

<p align="center">
  <img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/area2-radius60-247-251-252.gif" alt="Burla dashboard showing live logs, output files, and cluster status" />
</p>

# Examples

<table>
  <tr>
    <td width="33%" align="center"><a href="https://docs.burla.dev/examples/process-2.4tb-of-parquet-files-in-76s"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/query-2-4tb-parquet-card.png" width="100%" alt="Query 2.4TB of Parquet in 76s"><br><b>Query 2.4TB of Parquet in 76s</b></a></td>
    <td width="33%" align="center"><a href="https://docs.burla.dev/demo-blogs/airbnb-burla"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/airbnb-burla-card.png" width="100%" alt="Rank 1.7M Airbnbs"><br><b>Rank 1.7M Airbnbs</b></a></td>
    <td width="33%" align="center"><a href="https://docs.burla.dev/demo-blogs/amazon-review-distiller"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/amazon-review-distiller-card.png" width="100%" alt="Distill 572M Amazon reviews"><br><b>Distill 572M Amazon reviews</b></a></td>
  </tr>
  <tr>
    <td width="33%" align="center"><a href="https://docs.burla.dev/demo-blogs/arxiv-fossils"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/arxiv-fossils-card.png" width="100%" alt="Cluster 2.7M arXiv abstracts"><br><b>Cluster 2.7M arXiv abstracts</b></a></td>
    <td width="33%" align="center"><a href="https://docs.burla.dev/examples/multi-stage-genomic-pipeline"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/multi-stage-genomic-pipeline-card.png" width="100%" alt="Genomic alignment pipeline"><br><b>Genomic alignment pipeline</b></a></td>
    <td width="33%" align="center"><a href="https://docs.burla.dev/demo-blogs/world-photo-index"><img src="https://raw.githubusercontent.com/Burla-Cloud/user-docs/main/.gitbook/assets/more-examples/world-photo-index-card.png" width="100%" alt="Map 9.5M geotagged photos"><br><b>Map 9.5M geotagged photos</b></a></td>
  </tr>
</table>

<p align="center"><a href="https://docs.burla.dev/demo-categories/basic-examples"><b>Browse all examples &rarr;</b></a></p>

### Want to learn more? [Book a call](https://cal.com/jakez/burla?user=jakez), we'd love to chat.
