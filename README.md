<p align="center"><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/main/media/readme_banner.png" width=1000></p>

[Watch our 2min demo on YouTube!](https://www.youtube.com/watch?v=1HQkTL-7_VY)


#### Burla is a library for running python functions on remote computers.

Burla only has one function: **remote_parallel_map**.  
Given some python function, and a list of arguments, `remote_parallel_map` calls the given function, on every argument in the list, at the same time, each on a separate virtual machine in the cloud.

Here's an example:
```python
from burla import remote_parallel_map

my_arguments = [1, 2, 3, 4]

def my_function(my_argument: int):
    print(f"Running remote computer #{my_argument} in the cloud!")
    return my_argument * 2
    
results = remote_parallel_map(my_function, my_arguments)

print(f"return values: {list(results)}")
```

In the above example, each call to `my_function` runs on a separate virtual machine, in parallel.  
With Burla, running code on remote computers feels the same as running locally. This means:
- Any errors your function throws will appear on local machine just like they normally do.
- Anything you print appears in your local stdout, just like it normally does.
- responses are pretty quick (you can call a million simple functions in a couple seconds).

[Click here to learn more about remote_parallel_map.](https://docs.burla.dev/overview)

#### Introduction to Burla Clusters:
Burla is open-source cluster-compute software designed to be self-hosted in the cloud.  
To use Burla you must have a cluster running that the client knows about.   
Burla clusters are multi-tenant / can run many jobs from separate users.  
Nodes in a burla cluster are single-tenant / your job will never be on the same machine as another job.

[Click here to learn more about how burla-clusters work.](https://docs.burla.dev/how-does-it-work)

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