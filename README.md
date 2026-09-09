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

Burla runs Python functions in parallel across thousands of CPUs or GPUs in your cloud. Scale data processing, ML inference, and multi-stage pipelines with one function:

```python
from burla import remote_parallel_map

def double(x):
    return x * 2

results = remote_parallel_map(
    double, range(1000), grow=True
)
```

## Key features

- **Fast iteration.** Dispatch to 1,000 CPUs in under a second on a warm cluster. Prints, exceptions, and results appear locally.
- **Automatic dependencies.** Local Python modules ship with your function; missing packages install automatically. Bring a custom container image when needed.
- **Efficient compute.** Burla adjusts concurrency around CPU and memory use to keep machines busy. Specify hardware, including GPUs, in code.
- **Plain-Python pipelines.** Nest `remote_parallel_map` calls; Burla builds a live graph of your running jobs.
- **Your cloud.** Run in your own AWS, Google Cloud, or Azure account. Share a deployed cluster with your team.

## See it in action

Track every call, inspect logs and tracebacks, and spot resource bottlenecks in the dashboard.

[![Burla dashboard tour: live job progress, a failed call's logs, and CPU, memory, network, and disk usage](docs/assets/dashboard-demo.gif)](https://burla.dev/#what)

## Get started

With Python 3.11+ and [your cloud credentials configured](https://burla.dev/docs/get-started):

```bash
pip install burla
burla dashboard
```

Run the Python example above. `grow=True` starts workers in your cloud and removes them when the job finishes. For a shared cluster and background jobs, run `burla deploy`.

[Setup guide](https://burla.dev/docs/get-started) · [Examples](https://burla.dev/docs/examples) · [API reference](https://burla.dev/docs/api-reference)

## Contributing

Bug reports, feature requests, and contribution proposals are welcome in [GitHub issues](https://github.com/Burla-Cloud/burla/issues). Report security issues to security@burla.dev.

## License

Licensed under [FSL-1.1-Apache-2.0](LICENSE). Free to use except for competing commercial offerings; each version becomes Apache 2.0 after two years.

---

<p align="center">
  Questions? Email <a href="mailto:jake@burla.dev">jake@burla.dev</a> or <a href="https://cal.com/jakez/burla?user=jakez">book a call</a>, we're always happy to talk.
</p>
