# Getting Started with Grow

`Grow` is the `grow` argument on `remote_parallel_map`.

The short version is simple:

- Leave `grow=False` when you want to use only the cluster capacity that is already available.
- Set `grow=True` when you want Burla to add capacity automatically for this run.

If you remember one thing, remember this:

> `grow=True` means "this job is allowed to expand the cluster so it can reach the level of parallelism I asked for."

That makes Grow especially useful for bursty workloads: jobs that are occasionally large, important to finish quickly, and not worth keeping a big cluster warm for all day.

---

## What Grow actually does

When you call `remote_parallel_map`, Burla first looks at the capacity that is already ready in the cluster.

If `grow=False`, Burla uses only that existing capacity.

If `grow=True`, Burla does one more thing:

1. It looks at the parallelism you asked for.
2. It checks whether the current cluster can provide that much parallelism.
3. If the cluster is too small, it requests additional nodes.
4. Ready nodes can start working immediately while new nodes boot.

Grow does **not** change your function code.
Grow does **not** change your results.
Grow only changes how much infrastructure Burla is allowed to create for the job.

---

## Why Grow is useful

Grow is useful because most real workloads are not perfectly steady.

You often have jobs like:

- a batch of 500 images that needs processing right now
- a document enrichment run that is small most days and huge on Fridays
- an embeddings job that should finish quickly, but only runs a few times per day

Without Grow, you have to decide ahead of time how large the cluster should already be.
With Grow, you can keep the cluster smaller most of the time and let Burla expand it when a job needs more capacity.

That gives you three practical benefits:

1. **Less pre-planning**
   You do not need to warm the cluster manually before every large run.

2. **Faster burst handling**
   A job can start with the nodes that are ready and pick up more speed as additional nodes come online.

3. **Cleaner application code**
   Your function stays the same. The only change is one argument on `remote_parallel_map`.

---

## When to use Grow

Use `grow=True` when all three of these are true:

- you want the job to finish as quickly as reasonably possible
- the cluster may not already be large enough
- you know the maximum concurrency you actually want

In practice, Grow is a great fit for:

- bursty batch jobs
- scheduled jobs with uneven sizes
- workloads where the cluster might be idle or small between runs
- jobs where waiting for fixed cluster capacity would be noticeably slower

---

## When Grow is not the right tool

Grow is not always necessary.

Leave it off when:

- the cluster is already sized exactly how you want
- you want this job to stay inside current capacity
- the job is so small that extra booted nodes would not matter much

One subtle point is worth knowing:

If the job is extremely short, the additional nodes may finish booting after most of the work is already done.
That is still safe, but it means Grow may not materially reduce runtime for very tiny or very fast jobs.

---

## A simple example

Imagine you occasionally need to resize a large batch of product images.
The function itself is simple, but the batch size is unpredictable.
Some runs have 20 images. Others have 200.

Here is a clean, production-style starting point:

```py
from time import sleep

from burla import remote_parallel_map

IMAGE_COUNT = 200
PARALLELISM_LIMIT = 64


def resize_product_image(image_name: str) -> dict[str, str]:
    sleep(2)
    return {
        "image_name": image_name,
        "status": "resized",
    }


image_names = [f"product-image-{index:03d}.jpg" for index in range(IMAGE_COUNT)]

processed_images = remote_parallel_map(
    resize_product_image,
    image_names,
    max_parallelism=PARALLELISM_LIMIT,
    grow=True,
)
```

### Why this example is a good fit for Grow

This job has three properties that make Grow useful:

1. **The work is independent**  
   Each image can be processed on its own machine without coordination.

2. **The batch can be large**  
   Running 200 inputs with low parallelism would take much longer.

3. **You want bounded elasticity**  
   `max_parallelism=64` tells Burla, "Go fast, but do not exceed 64 concurrent function calls."

If the cluster already has enough ready capacity for 64 concurrent calls, `grow=True` will not change much.
If it does not, Burla can add nodes so the run can move toward that 64-way parallelism target.

---

## The same call without Grow

Here is the same job without the Grow argument:

```py
from time import sleep

from burla import remote_parallel_map

IMAGE_COUNT = 200
PARALLELISM_LIMIT = 64


def resize_product_image(image_name: str) -> dict[str, str]:
    sleep(2)
    return {
        "image_name": image_name,
        "status": "resized",
    }


image_names = [f"product-image-{index:03d}.jpg" for index in range(IMAGE_COUNT)]

processed_images = remote_parallel_map(
    resize_product_image,
    image_names,
    max_parallelism=PARALLELISM_LIMIT,
)
```

This version says:

> "Use up to 64-way parallelism if the cluster already has that capacity."

The earlier version says:

> "Use up to 64-way parallelism, and if the cluster is too small, grow it for this run."

That difference is the entire point of Grow.

---

## How to think about `max_parallelism`

Grow works best when you pair it with a deliberate `max_parallelism`.

That is important because `grow=True` is not a vague request to "make it faster."
It is permission for Burla to add capacity until the job can approach the concurrency you asked for.

A good mental model is:

- `inputs` says how much total work exists
- `max_parallelism` says how much of that work may run at once
- `grow=True` says Burla may expand the cluster to reach that level

If you omit `max_parallelism`, Burla defaults to the number of inputs.
That can be exactly what you want, but for many production jobs it is better to set an explicit concurrency limit.
If you pass 10,000 inputs and leave `max_parallelism` unset, you are effectively saying that 10,000-way parallelism is acceptable if the cluster can provide it.

For example:

```py
processed_images = remote_parallel_map(
    resize_product_image,
    image_names,
    max_parallelism=64,
    grow=True,
)
```

This is usually a better production pattern than relying on the default when you want predictable scaling behavior.

---

## How Grow interacts with `func_cpu` and `func_ram`

Grow does not only look at input count.
It also respects the resources each function call needs.

If your function needs larger hardware, Burla uses that when deciding whether more nodes are required.

For example:

```py
processed_images = remote_parallel_map(
    resize_product_image,
    image_names,
    max_parallelism=32,
    func_cpu=2,
    func_ram=8,
    grow=True,
)
```

This says:

- run at most 32 function calls at once
- give each call 2 CPUs and 8 GB of RAM
- add nodes if the cluster cannot currently support that

This is one of the biggest reasons Grow is useful.
You do not have to manually calculate cluster size every time you change concurrency or hardware requirements.

---

## What happens during a Grow-enabled run

The behavior is best understood as a sequence:

1. Burla checks the currently available nodes.
2. It assigns as much work as it can to ready nodes.
3. If capacity is missing and `grow=True`, it asks the cluster to add more nodes.
4. New nodes join the job as they become ready.
5. The run finishes with the same outputs you would get without Grow.

That means Grow is elastic, not magical.
New nodes still need time to boot.
Grow improves throughput when extra capacity matters, but it does not teleport a large cluster into existence instantly.

Burla currently caps Grow at 2,560 CPUs for a job.

---

## A practical decision rule

If you want a simple rule you can apply quickly, use this one:

### Turn Grow on

Use `grow=True` when you would say:

> "This job should run fast, and I do not want to depend on the cluster already being large enough."

### Leave Grow off

Leave `grow=False` when you would say:

> "This job should stay within the capacity that is already available."

That framing is usually enough to make the right choice.

---

## Recommended default for bursty jobs

For bursty workloads, this is a strong default pattern:

```py
from burla import remote_parallel_map


results = remote_parallel_map(
    your_function,
    inputs,
    max_parallelism=your_parallelism_limit,
    grow=True,
)
```

That gives you:

- a clear concurrency ceiling
- automatic cluster expansion when needed
- simple, stable application code

---

## Quick reference

| Situation | Use `grow=True`? | Why |
| --- | --- | --- |
| The cluster might be off or undersized | Yes | Burla can add capacity for this run |
| You have bursty batch workloads | Yes | Faster runs without keeping the cluster large all the time |
| The cluster is already sized exactly how you want | Usually no | Growth is unnecessary |
| You want strict fixed-capacity behavior | No | Leave the job inside existing capacity |
| The job is tiny and finishes very quickly | Usually no | Extra booted nodes may not help much |

---

## Final takeaway

Grow is best thought of as **elastic parallelism for a single job**.

It is not about changing your function.
It is about changing whether Burla is allowed to add infrastructure so that function can run at the level of parallelism you requested.

If your workload is bursty, if speed matters, and if you do not want to pre-size the cluster manually, `grow=True` is exactly what you want.
