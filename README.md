### Run any Python function on 1000 computers in 1 second.

Burla is the simplest way to run Python on lot's of computers in the cloud.

<figure><img src="https://github.com/Burla-Cloud/.github/blob/main/media/main_demo.gif" alt="" style="width:70%" /><figcaption></figcaption></figure>

#### How It Works:

Burla only has one function:

```python
from burla import remote_parallel_map

my_inputs = [1, 2, 3]

def my_function(my_input):
    print("I'm running on my own separate computer in the cloud!")
    return my_input
    
return_values = remote_parallel_map(my_function, my_inputs)
```

Running code in the cloud feels the same as running code locally:

* Anything you print appears in your local terminal.
* Exceptions thrown in your code are thrown on your local machine.
* Responses are pretty quick, you can call a million simple functions in a couple seconds.

#### Attach Big Hardware to Functions That Need It:

Zero config files, just simple arguments like `func_cpu` & `func_ram`.

```python
from xgboost import XGBClassifier

def train_model(hyper_parameters):
    model = XGBClassifier(n_jobs=64, **hyper_parameters)
    model.fit(training_inputs, training_targets)
    
remote_parallel_map(train_model, parameter_grid, func_cpu=64, func_ram=256)
```

#### A Fast, Scalable Task Queue:

Queue up 10 Million function calls, and run them with thousands of containers.\
Our custom distributed task queue is incredibly fast, keeping hardware utilization high.

<figure><img src="https://github.com/Burla-Cloud/.github/blob/main/media/queue_demo.gif" alt="" style="width:80%" /><figcaption><p>This demo is in realtime!</p></figcaption></figure>

#### Simple, Flexible Pipelines:

Nest `remote_parallel_map` calls to build simple, massively parallel pipelines.\
Use `background=True` to fire and forget code, then monitor progress from the dashboard.

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

#### Run Code in any Docker Image, on any Hardware:

Public or private, just paste a link to your image and hit start.\
Scale to 10,000 CPU's, terabytes of RAM, or 1,000 H100's, everything stays in your cloud.

<figure><img src="https://github.com/Burla-Cloud/.github/blob/main/media/settings_demo.gif" alt="" style="width:80%" /><figcaption></figcaption></figure>

#### Deploy With Just Two Commands:

(**Burla is currently Google Cloud only!**)

1. `pip install burla`
2. `burla install`

See the [Getting Started guide](https://docs.burla.dev/getting-started#quickstart) for more info.


&nbsp;
&nbsp;


***

Questions?\
[Schedule a call](http://cal.com/jakez/burla), or email **jake@burla.dev**. We're always happy to talk.
