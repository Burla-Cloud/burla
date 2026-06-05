"""
Smoke test for nested remote_parallel_map calls in remote-dev mode on Burla 1.5.10.

Run inside the dev VM with:
    BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001 \
        uv run --project ./client --group dev python nested_rpm_smoke.py

It runs an outer remote_parallel_map. Each outer worker calls remote_parallel_map
again over a small inner range. If nested RPM is broken, the outer job surfaces an
exception originating inside the inner call.
"""

import sys
import traceback

import pandas as pd

from burla import remote_parallel_map


def inner_udf(x):
    return float(pd.Series([x, x, x]).mean()) ** 2


def outer_udf(n):
    inputs = list(range(n))
    results = list(remote_parallel_map(inner_udf, inputs, spinner=False))
    return float(pd.Series(results).sum())


def main():
    print("=== outer remote_parallel_map starting ===", flush=True)
    try:
        outputs = list(
            remote_parallel_map(
                outer_udf, [3, 4], func_cpu=2, grow=True, spinner=False
            )
        )
    except BaseException as exc:
        print("=== outer remote_parallel_map raised ===", flush=True)
        traceback.print_exc()
        sys.exit(2)
    print("=== outer outputs ===", flush=True)
    for output in outputs:
        print(output, flush=True)


if __name__ == "__main__":
    main()
