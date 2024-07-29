import asyncio
from urllib.parse import quote

import cloudpickle
import nest_asyncio
from aiohttp import ClientSession


nest_asyncio.apply()


class StatusMessage:
    function_name = None
    total_cpus = None
    total_gpus = None
    n_inputs = None

    uploading_inputs = "Uploading Inputs ..."
    uploading_function = "Uploading Function ..."
    downloading = "Downloading Outputs ..."

    @classmethod
    def preparing(cls):
        msg = f"Preparing to run {cls.n_inputs} inputs through `{cls.function_name}` with "
        if cls.total_gpus > 0:
            msg += f"{cls.total_cpus} CPUs, and {cls.total_gpus} GPUs."
        else:
            msg += f"{cls.total_cpus} CPUs."
        return msg

    @classmethod
    def running(cls):
        msg = f"Running {cls.n_inputs} inputs through `{cls.function_name}` with {cls.total_cpus} "
        msg += f"CPUs, and {cls.total_gpus} GPUs." if cls.total_gpus > 0 else "CPUs."
        return msg


class JobTimeoutError(Exception):
    def __init__(self, job_id, timeout):
        super().__init__(f"Burla job with id: '{job_id}' timed out after {timeout} seconds.")


class InstallError(Exception):
    def __init__(self, stdout: str):
        super().__init__(
            f"The following error occurred attempting to pip install packages:\n{stdout}"
        )


class ServerError(Exception):
    def __init__(self):
        super().__init__(
            (
                "An unknown error occurred in Burla's cloud, this is not an error with your code. "
                "Someone has been notified, please try again later."
            )
        )


def nopath_warning(message, category, filename, lineno, line=None):
    return f"{category.__name__}: {message}\n"


def upload_inputs(job_id: str, pickled_inputs: list, gcs_auth_headers: dict, jobs_bucket: str):
    async def _upload_input(session, url, input_):
        async with session.request(method="post", url=url, data=input_) as response:
            response.raise_for_status()

    async def _make_requests(urls, inputs, headers):
        async with ClientSession(headers=headers) as session:
            tasks = [_upload_input(session, url, input_) for url, input_ in zip(urls, inputs)]
            await asyncio.gather(*tasks)

    base_url = "https://www.googleapis.com/upload/storage"
    name_to_url = lambda name: f"{base_url}/v1/b/{jobs_bucket}/o?uploadType=media&name={name}"
    urls = [name_to_url(f"{job_id}/inputs/{i}.pkl") for i in range(len(pickled_inputs))]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_make_requests(urls, pickled_inputs, gcs_auth_headers))


def download_outputs(job_id: str, num_outputs: int, gcs_auth_headers: dict, jobs_bucket: str):

    async def _download_output(session, url, remaining_attempts=100):
        async with session.request(method="get", url=url) as response:
            if response.status == 404 and remaining_attempts > 0:
                return await _download_output(session, url, remaining_attempts - 1)
            response.raise_for_status()
            return await response.read()

    async def _make_requests(urls, headers):
        async with ClientSession(headers=headers) as session:
            tasks = [_download_output(session, url) for url in urls]
            return await asyncio.gather(*tasks)

    base_url = "https://www.googleapis.com/storage/v1"
    name_to_url = lambda name: f"{base_url}/b/{jobs_bucket}/o/{quote(name, safe='')}?alt=media"
    urls = [name_to_url(f"{job_id}/outputs/{i}.pkl") for i in range(num_outputs)]

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(_make_requests(urls, gcs_auth_headers))
    return [cloudpickle.loads(result) for result in results]
