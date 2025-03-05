import sys
import base64
import pickle
import requests
from time import sleep
from typing import Union

import cloudpickle
from tblib import Traceback
from google.auth.transport.requests import Request
from worker_service import SELF, PROJECT_ID, CREDENTIALS

FIRESTORE_URL = "https://firestore.googleapis.com"
DB_BASE_URL = f"{FIRESTORE_URL}/v1/projects/{PROJECT_ID}/databases/burla/documents"


class EmptyInputQueue(Exception):
    pass


class InputGetter:
    """
    The policy implemented here is designed to minimize firestore document collisions.
    If too many workers try to grab a firestore document (input) at the same time, stuff breaks.
    """

    def __init__(
        self,
        db_headers: dict,
        inputs_id: str,
        num_inputs: int,
        starting_index: int,
        parallelism: int,
    ):
        self.db_headers = db_headers
        self.inputs_id = inputs_id
        self.num_inputs = num_inputs
        self.parallelism = parallelism
        self.starting_index = starting_index
        self.current_index = starting_index

    def __attempt_to_claim_input(self, input_index: int) -> Union[None, bytes]:
        batch_size = 100
        batch_min_index = (input_index // batch_size) * batch_size
        collection_name = f"{batch_min_index}-{batch_min_index + batch_size}"
        SELF["WORKER_LOGS"].append(f"attempting to claim input {input_index}")

        # grab doc
        input_pkl = None
        input_doc_url = f"{DB_BASE_URL}/inputs/{self.inputs_id}/{collection_name}/{input_index}"
        for i in range(15):
            response = requests.get(input_doc_url, headers=self.db_headers)
            if response.status_code == 200:
                input_pkl = base64.b64decode(response.json()["fields"]["input"]["bytesValue"])
                is_claimed = response.json()["fields"]["claimed"]["booleanValue"]
                break
            if response.status_code != 404:
                try:
                    response.raise_for_status()
                except Exception as e:
                    msg = f"non 404/200 response trying to get input {input_index}?"
                    raise Exception(msg) from e
            sleep(i * i * 0.1)  # 0.0, 0.1, 0.4, 0.9, 1.6, 2.5, 3.6, 4.9, 6.4, 8.1 ...

        if not input_pkl:
            raise Exception(f"DOCUMENT #{input_index} NOT FOUND after 10 attempts.")
        elif is_claimed:
            return None

        # mark doc as claimed
        SELF["WORKER_LOGS"].append(f"input found, marking input {input_index} as claimed")
        update_claimed_url = f"{input_doc_url}?updateMask.fieldPaths=claimed"
        data = {"fields": {"claimed": {"booleanValue": True}}}
        response = requests.patch(update_claimed_url, headers=self.db_headers, json=data)
        response.raise_for_status()
        SELF["WORKER_LOGS"].append(f"successfully marked input {input_index} as claimed")
        return input_pkl

    def __get_next_index(self, old_index: int):
        new_index = (old_index + self.parallelism) % self.num_inputs
        if (new_index == self.starting_index) or (new_index == old_index):
            new_index = (new_index + 1) % self.num_inputs
            self.starting_index += 1
        SELF["WORKER_LOGS"].append(f"computed new index: old={old_index}, new={new_index}")
        return new_index

    def get_next_input(self, attempt=0):

        input_pkl = self.__attempt_to_claim_input(self.current_index)
        input_index = self.current_index
        self.current_index = self.__get_next_index(old_index=self.current_index)

        if input_pkl:
            return input_index, input_pkl
        elif attempt == 5:
            # If I try to claim 5 documents and they are all claimed then every document was
            # checked 5 times and this worker can DEFINITELY stop trying! ?
            # (or another worker is 5 docs ahead of this one and covering for it / will continue to)
            raise EmptyInputQueue()
        else:
            return self.get_next_input(attempt=attempt + 1)


class _FirestoreLogger:

    def __init__(self, job_id: str, db_headers: dict, input_index: int):
        self.job_id = job_id
        self.db_headers = db_headers
        self.input_index = input_index

    def write(self, msg):
        if msg.strip() and (len(msg.encode("utf-8")) > 1_048_376):  # (1mb - est overhead):
            msg_truncated = msg.encode("utf-8")[:1_048_376].decode("utf-8", errors="ignore")
            msg = msg_truncated + "<too-long--remaining-msg-truncated-due-to-length>"
        if msg.strip():
            log_doc_url = f"{DB_BASE_URL}/jobs/{self.job_id}/logs"
            data = {"fields": {"msg": {"stringValue": msg}}}
            response = requests.post(log_doc_url, headers=self.db_headers, json=data)
            response.raise_for_status()

    def flush(self):
        self.original_stdout.flush()

    def __enter__(self):
        self.original_stdout = sys.stdout
        sys.stdout = self

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self.original_stdout


def _serialize_error(exc_info):
    # exc_info is tuple returned by sys.exc_info()
    exception_type, exception, traceback = exc_info
    pickled_exception_info = pickle.dumps(
        dict(
            type=exception_type,
            exception=exception,
            traceback_dict=Traceback(traceback).to_dict(),
        )
    )
    return pickled_exception_info


def execute_job(
    job_id: str,
    inputs_id: str,
    n_inputs: int,
    starting_index: int,
    planned_future_job_parallelism: int,
    function_pkl: bytes,
):
    CREDENTIALS.refresh(Request())
    db_headers = {
        "Authorization": f"Bearer {CREDENTIALS.token}",
        "Content-Type": "application/json",
    }

    input_getter = InputGetter(
        db_headers,
        inputs_id=inputs_id,
        num_inputs=n_inputs,
        starting_index=starting_index,
        parallelism=planned_future_job_parallelism,
    )

    user_defined_function = None
    while True:

        try:
            input_index, input_pkl = input_getter.get_next_input()
        except EmptyInputQueue:
            SELF["DONE"] = True
            SELF["WORKER_LOGS"].append(f"Input queue is empty.\nDone executing job: {job_id}.")
            return

        # run UDF:
        exec_info = None
        with _FirestoreLogger(job_id, db_headers, input_index):
            try:
                if user_defined_function is None:
                    user_defined_function = cloudpickle.loads(function_pkl)
                input_ = cloudpickle.loads(input_pkl)
                return_value = user_defined_function(input_)
            except Exception:
                exec_info = sys.exc_info()

        # serialize result:
        result_pkl = _serialize_error(exec_info) if exec_info else cloudpickle.dumps(return_value)
        result_too_big = len(result_pkl) > 1_048_376
        if result_too_big:
            noun = "Error" if exec_info else "Return value"
            msg = f"{noun} from input at index {input_index} is greater than 1MB in size."
            raise Exception(f"{msg}\nUnable to store result.")

        # write result:
        result_doc_url = f"{DB_BASE_URL}/jobs/{job_id}/results/{input_index}"
        encoded_result_pkl = base64.b64encode(result_pkl).decode("utf-8")
        data = {
            "fields": {
                "is_error": {"booleanValue": bool(exec_info)},
                "result_pkl": {"bytesValue": encoded_result_pkl},
            }
        }
        response = requests.patch(result_doc_url, headers=db_headers, json=data)
        response.raise_for_status()
