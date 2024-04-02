import subprocess
import warnings
import json
import sys
import types

from burla._helpers import nopath_warning

warnings.formatwarning = nopath_warning


class EnvironmentInspectionError(Exception):
    def __init__(self, stdout):
        super().__init__(
            (
                "The following error occurred attempting to get list if packages to install in "
                f"remove execution environment's: {stdout}"
            )
        )


def get_pip_packages():
    result = subprocess.run(
        ["pip", "list", "--format=json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if result.returncode != 0:
        raise EnvironmentInspectionError(result.stderr)

    for pkg in json.loads(result.stdout):
        if "+" in pkg["version"]:
            pkg["version"] = pkg["version"].split("+")[0]
        if not pkg.get("editable_project_location"):
            yield pkg


def get_function_dependencies(function_):
    """Returns python modules imported in module where `function_` is defined"""
    function_module_name = function_.__module__
    function_module = sys.modules[function_module_name]

    for obj_name in dir(function_module):
        obj = getattr(function_module, obj_name)
        if isinstance(obj, types.ModuleType):
            yield obj.__name__.split(".")[0]
        elif hasattr(obj, "__module__"):
            yield obj.__module__.split(".")[0]
