import sys
import traceback
from time import time
from threading import Thread
from typing import Optional, Callable

import docker
from fastapi import APIRouter, Depends, Response
from google.cloud import firestore

from node_service import (
    PROJECT_ID,
    SELF,
    INIT_SELF,
    INSTANCE_N_CPUS,
    INSTANCE_NAME,
    IN_LOCAL_DEV_MODE,
    get_logger,
    get_add_background_task_function,
    Container,
)
from node_service.helpers import Logger, format_traceback, ignore_400_409_404
from node_service.worker import Worker

router = APIRouter()


@router.post("/background_reboot")
def background_reboot(
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    add_background_task(reboot_containers, logger=logger)


@router.post("/reboot")
def reboot_containers(
    new_container_config: Optional[list[Container]] = None,
    logger: Logger = Depends(get_logger),
):
    """
    Rebooting will reboot the containers that are currently/ were previously running.
    If new containers are passed with the reboot request, those containers will be booted instead.
    """
    started_booting_at = time()

    # TODO: seems to have like a 1/5 chance (only after running a job) of throwing a:
    # `Unable to reboot, not all containers started!`
    # Error, prececed by many `PORT ALREADY IN USE, TRYING AGAIN.`'s
    # This only happens with a high number of containers.

    if SELF["BOOTING"]:
        return Response("Node already BOOTING, unable to satisfy request.", status_code=409)

    try:
        logger.log(f"REBOOTING NODE: {INSTANCE_NAME}")
        SELF = INIT_SELF()
        if new_container_config:
            SELF["current_container_config"] = new_container_config

        docker_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        node_doc = (
            firestore.Client(project=PROJECT_ID, database="burla")
            .collection("nodes")
            .document(INSTANCE_NAME)
        )
        node_doc.update(
            {
                "status": "BOOTING",
                "current_job": None,
                "parallelism": None,
                "target_parallelism": None,
                "started_booting_at": started_booting_at,
                "all_inputs_received": False,
            }
        )

        threads = []
        if IN_LOCAL_DEV_MODE:
            # allow us to read old worker logs before workers are deleteted.
            # workers are marked as "old", then deleted in subsequent restart.
            # `just_marked_old`/`worker_just_created` exist to prevent race condition where
            # one node_service makes workers and the other instantly marks them old/deletes them.
            workers_marked_old = []
            old_workers_removed = []

            # REGARDING `ignore_400_409_404`:
            # We assume that when these errors occur it is always because the operation has already
            # been completed/is-happening by another thread.

            for container in docker_client.containers(all=True):
                name = container["Names"][0][1:]
                is_worker = "worker" in name
                is_old = name.startswith("OLD")
                worker_belongs_to_self = f"node_{INSTANCE_NAME[11:]}" in name

                if is_old and worker_belongs_to_self:
                    remove_container = ignore_400_409_404(docker_client.remove_container)
                    kwargs = {"container": container["Id"], "force": True}
                    threads.append(Thread(target=remove_container, kwargs=kwargs))
                    old_workers_removed.append(name)

                elif is_worker and worker_belongs_to_self:
                    rename_container = ignore_400_409_404(docker_client.rename)
                    stop_container = ignore_400_409_404(docker_client.stop)
                    rename_args = (container["Id"], f"OLD--{name}")
                    stop_kwargs = {"container": container["Id"], "timeout": 0}
                    threads.append(Thread(target=rename_container, args=rename_args))
                    threads.append(Thread(target=stop_container, kwargs=stop_kwargs))
                    workers_marked_old.append(name)

            # logger.log(f'Marked {len(workers_marked_old)} workers as "OLD": {workers_marked_old}')
            # logger.log(f'Removed {len(old_workers_removed)} "OLD" workers: {old_workers_removed}')
        else:
            # remove all worker containers
            workers_removed = []
            for container in docker_client.containers():
                if "worker" in container["Names"][0]:
                    remove_container = ignore_400_409_404(docker_client.remove_container)
                    kwargs = {"container": container["Id"], "force": True}
                    threads.append(Thread(target=remove_container, kwargs=kwargs))
                    workers_removed.append(container["Names"][0])

            # logger.log(f"Removed {len(workers_removed)} workers: {workers_removed}")

        [thread.start() for thread in threads]
        [thread.join() for thread in threads]

        def create_worker(*a, **kw):
            # Log error inside thread because sometimes it isn't sent to the main thread, idk why.
            try:
                worker = Worker(*a, **kw)
                SELF["workers"].append(worker)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                traceback_str = format_traceback(tb_details)
                logger.log(str(e), "ERROR", traceback=traceback_str)

        # start one container per container-config per cpu
        # max num containers is 1024 due to some network/port related limit
        threads = []
        for container_spec in SELF["current_container_config"]:
            for i in range(INSTANCE_N_CPUS):
                # have only one worker send logs to gcl, too many will break gcl
                send_logs_to_gcl = False  # (i == 0) and (not IN_LOCAL_DEV_MODE)
                args = (
                    container_spec.python_version,
                    container_spec.python_executable,
                    container_spec.image,
                    docker_client,
                    send_logs_to_gcl,
                )
                thread = Thread(target=create_worker, args=args)
                threads.append(thread)
                thread.start()

        [thread.join() for thread in threads]
        # worker_names = [w.container_name for w in SELF["workers"]]
        # logger.log(f'Started {len(SELF["workers"])} new workers: {worker_names}')

        # Sometimes on larger machines, some containers don't start, or get stuck in "CREATED" state
        # This has not been diagnosed, this check is performed to ensure all containers started.
        _containers = docker_client.containers()
        is_worker_container = lambda c: c["Names"][0][1:13] not in ["main_service", "node_service"]
        worker_containers_status = [c["State"] for c in _containers if is_worker_container(c)]
        num_running_containers = sum([status == "running" for status in worker_containers_status])
        expected_n_containers = len(SELF["current_container_config"]) * INSTANCE_N_CPUS
        some_containers_missing = num_running_containers < expected_n_containers

        if some_containers_missing:
            SELF["FAILED"] = True
            node_doc.update({"status": "FAILED"})
            raise Exception("Unable to reboot, not all containers started!")
        else:
            SELF["BOOTING"] = False
            node_doc.update({"status": "READY"})

    except Exception as e:
        SELF["FAILED"] = True
        raise e

    logger.log(f"Done Rebooting.\n{INSTANCE_NAME} is READY!")
