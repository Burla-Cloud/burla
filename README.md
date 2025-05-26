<p align="center"><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/main/media/readme_banner.png" width=1000></p>


#### Burla is an open-source, batch-processing platform for Python developers.

- Burla can deploy a simple python function to 10,000VM's in about 2 seconds (see our [demo](https://www.youtube.com/watch?v=1HQkTL-7_VY)).
- Code runs in any docker container, on any machine type, for any length of time.
- It comes with a dashboard to monitor long running jobs, and view logs.
- Burla can be installed with one command.

#### Burla is a python package with only one function:
```python
from burla import remote_parallel_map
​
​
def my_function(my_input):
    print("I'm running on remote computer in the cloud!")
    
remote_parallel_map(my_function, [1, 2, 3])
```

This code runs: `my_function(1)`, `my_function(2)`, `my_function(3)` in parallel, each in a separate container, and on a separate cpu, in the cloud.
With Burla, running code in the cloud feels the same as coding locally:
- Anything you print appears your local terminal.
- Exceptions thrown in your code are thrown on your local machine.
- Responses are pretty quick, you can call a million simple functions in a couple seconds.

[Click here to learn more about remote_parallel_map.](https://docs.burla.dev/overview)

#### Developing:

If you want to contribute, send me an [email](mailto:jake@burla.dev)!  
We are still early so there is quite a bit of undocumented / manual setup necessary to get a dev environment going.  
Please read the [how-it-works](https://docs.burla.dev/how-does-it-work) page as a prerequisite.
  
Here is a quick attempt to explain how dev works. The cluster can be run in three modes:
- `local dev mode`:  
    In this mode the entire cluster runs locally, each service runs in it's own docker container, in the docker-network "local-burla-cluster". This mode does NOT use docker-compose. This is the most common mode used when devloping burla.  
    Anytime some file is saved:
    - The dashboard builds itself into the `main_service/src/main_service/static` folder (see `main_service` readme for setup).
    - The appropriate service reloads itself (eg: if a file in the `node_service` is saved, any node service's reload themself).  
- `remote dev mode`:  
    In this mode only the main service runs locally, the node and worker services run in the cloud in the same way they would in a prod scenario. Useful when testing features / issues that only appear at scale.  
- `production`:  
    Everything runs in the cloud.

&nbsp;
&nbsp;

---
Questions?
[Schedule a call with us](https://cal.com/jakez/burla?duration=30), or [email us](mailto:jake@burla.dev). We're always happy to talk.