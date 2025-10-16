### Run any Python function on 1000 computers in 1 second.

Burla is the world's simplest cluster compute software.\
It's open-source, works with GPU's, custom containers, and up to 10,000 CPU's at once.

<figure><img src="https://github.com/Burla-Cloud/.github/blob/main/media/main_demo.gif" alt="" style="width:70%" /><figcaption></figcaption></figure>

### A fully fledged data-platform any team can learn in minutes:

Burla comes with a simple web-platform so your entire team can schedule jobs, create pipelines, scale machine learning systems, or other research efforts without weeks of onboarding or setup.

<figure><img src="https://github.com/Burla-Cloud/.github/blob/main/media/FINAL-lowfr.gif"" alt="" style="width:70%" /><figcaption></figcaption></figure>

### **How it works:**

Burla only has one function:

```python
from burla import remote_parallel_map

my_inputs = [1, 2, 3]

def my_function(my_input):
    print("I'm running on my own separate computer in the cloud!")
    return my_input
    
return_values = remote_parallel_map(my_function, my_inputs)
```

With Burla, running code in the cloud feels the same as coding locally:

* Anything you print appears in your local terminal.
* Exceptions thrown in your code are thrown on your local machine.
* Your local python packages are automatically synchronized with the cluster.
* Responses are pretty quick, you can call a million simple functions in a couple seconds!

### Attach big hardware to functions that need it:

Zero config files, just simple arguments like `func_cpu` & `func_ram`.

```python
from xgboost import XGBClassifier

def train_model(hyper_parameters):
    model = XGBClassifier(n_jobs=64, **hyper_parameters)
    model.fit(training_inputs, training_targets)
    
remote_parallel_map(train_model, parameter_grid, func_cpu=64, func_ram=256)
```

### Simple, flexible pipelines:

Nest `remote_parallel_map` calls to build simple, massively parallel pipelines.\
Use `background=True` to schedule function calls that keep running after you close your laptop.

```python
from burla import remote_parallel_map

def process_record(record):
    # Pretend this does some math per-record!
    return result

def process_file(file):
    results = remote_parallel_map(process_record, split_into_records(file))
    upload_results(results)

def process_files(files):
    remote_parallel_map(process_file, files, func_ram=16)
    

remote_parallel_map(process_files, [files], background=True)
```

### Run code in any Docker image, using the latest GPU's:

Public or private, just paste a URI to your image and hit start.\
Burla works with any linux based Docker image.

<figure><img src="https://github.com/Burla-Cloud/.github/blob/main/media/settings_demo.gif" alt="" style="width:80%" /><figcaption></figcaption></figure>

### Try it now

Email **jake@burla.dev** and we'll send you a free managed instance! Compute is on us.  
If you decide you like it, you can self-host Burla (currently Google Cloud only) with just two commands:

1. `pip install burla`  
2. `burla install`

See the [Getting Started guide](https://docs.burla.dev/getting-started#quickstart) for more info.

***

Questions?\
[Schedule a call](http://cal.com/jakez/burla), or email **jake@burla.dev**. We're always happy to talk.