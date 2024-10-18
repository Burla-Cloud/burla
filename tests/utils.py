import sys
import subprocess

from burla import remote_parallel_map


def with_packages(packages):
    def decorator(test_function):
        def wrapper(*args, **kwargs):
            install_cmd = [sys.executable, "-m", "pip", "install"] + packages
            subprocess.check_call(install_cmd)

            try:
                result = test_function(*args, **kwargs)
            finally:
                uninstall_cmd = [sys.executable, "-m", "pip", "uninstall", "-y"] + packages
                subprocess.check_call(uninstall_cmd)

            return result

        return wrapper

    return decorator
