"""
Subprocess entry point for the rpm_subprocess fixture.

Lives in a standalone module so `mp.get_context('spawn')` can pickle and
re-import it cleanly. Importing from `conftest` doesn't work because
pytest's conftest isn't on sys.path as a normal module.
"""

from __future__ import annotations

import contextlib
import io
import os
import traceback
from typing import Any


def run_rpm_in_subprocess(
    result_queue: Any,
    function_source: str,
    inputs: list,
    kwargs: dict,
    env_overrides: dict,
    dashboard_url: str,
) -> None:
    for k, v in env_overrides.items():
        os.environ[k] = v
    os.environ.setdefault("BURLA_CLUSTER_DASHBOARD_URL", dashboard_url)

    from burla import remote_parallel_map

    function_namespace: dict = {}
    exec(function_source, function_namespace, function_namespace)
    test_function = function_namespace["test_function"]

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            outputs = remote_parallel_map(test_function, inputs, **kwargs)
            if kwargs.get("generator"):
                outputs = list(outputs)
        result_queue.put(
            {
                "ok": True,
                "stdout": stdout_buffer.getvalue(),
                "stderr": stderr_buffer.getvalue(),
                "outputs": outputs,
            }
        )
    except BaseException as e:
        tb = traceback.format_exc()
        result_queue.put(
            {
                "ok": False,
                "stdout": stdout_buffer.getvalue(),
                "stderr": stderr_buffer.getvalue(),
                "exception_type": type(e).__name__,
                "exception_module": type(e).__module__,
                "exception_message": str(e),
                "traceback": tb,
                "burla_input_index": getattr(e, "burla_input_index", None),
            }
        )
