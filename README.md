<p align="center"><img src="https://raw.githubusercontent.com/Burla-Cloud/.github/main/media/readme_banner.png" width=1000></p>

#### Burla is a library for running python functions on remote computers.

Burla only has one function: **remote_parallel_map**.  
Given some python function, and a list of arguments, remote_parallel_map calls the given function, on every argument in the list, at the same time, each on a separate virtual machine in the cloud.

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

[Click here to run this example in Google Colab.](https://colab.research.google.com/drive/17MWiQFyFKxTmNBaq7POGL0juByWIMA3w?usp=sharing)

In the above example, each call to my_function runs on a separate virtual machine, in parallel.  
With Burla, running code on remote computers feels the same as running locally. This means:
- Any errors your function throws will appear on local machine just like they normally do.
- Anything you print appears in your local stdout, just like it normally does.
- responses are pretty quick (you can run a million simple functions in a couple seconds).

[Click here to learn more about remote_parallel_map.](https://docs.burla.dev/overview)

#### Where does my code run?
Burla is open-source cluster-compute software designed to be self-hosted in the cloud.  
To use Burla you must have a cluster running that the client knows about.  
Currently, our library is hardcoded to only call our free public cluster (cluster.burla.dev) which we've deployed to make Burla easy for anyone to try. This cluster is currently configured to run 16 nodes, each with 32 cpus & 128G ram.  
Burla clusters are multi-tenant/ can run many jobs from separate users.  
Nodes in a burla cluster are single-tenant/ your job will never be on the same machine as another job.

[Click here to learn more about how burla-clusters work.](https://docs.burla.dev/overview#how-does-it-work)

