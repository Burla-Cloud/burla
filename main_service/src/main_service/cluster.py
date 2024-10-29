from typing import List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
from copy import copy
import requests
from dataclasses import dataclass
from uuid import uuid4
from time import time

from google.cloud import firestore
from google.cloud.firestore import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from google.api_core.exceptions import NotFound

from main_service import PROJECT_ID
from main_service.node import Node, Container
from main_service.helpers import Logger


N_FOUR_STANDARD_CPU_TO_RAM = {2: 8, 4: 16, 8: 32, 16: 64, 32: 128, 48: 192, 64: 256, 80: 320}


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int):
    """What is the maximum number of parallel subjobs this machine_type can run a job with the
    following resource requirements at?
    """
    if machine_type.startswith("n4-standard") and machine_type.split("-")[-1].isdigit():
        vm_cpu = int(machine_type.split("-")[-1])
        vm_ram = N_FOUR_STANDARD_CPU_TO_RAM[vm_cpu]
        return min(vm_cpu // func_cpu, vm_ram // func_ram)
    raise ValueError(f"machine_type must be n4-standard-X")


@dataclass
class GCEBurlaNode:
    # Represents GCE VM Instance running as a node in a Burla cluster.
    name: str  # Instance Name
    zone: str  # CA Zone
    status: str  # Instance Status


def reboot_node(node_svc_host, node_containers):
    try:
        response = requests.post(f"{node_svc_host}/reboot", json=node_containers)
        response.raise_for_status()
    except Exception as e:
        # if node already rebooting, skip.
        if "409" not in str(e):
            raise e


def reboot_nodes_with_job(db: firestore.Client, job_id: str):
    nodes_with_job_filter = FieldFilter("current_job", "==", job_id)
    nodes_with_job = db.collection("nodes").where(filter=nodes_with_job_filter).stream()

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = []
        for node_snapshot in nodes_with_job:
            node_dict = node_snapshot.to_dict()
            future = executor.submit(reboot_node, node_dict["host"], node_dict["containers"])
            futures.append(future)

        # this raises any exceptions
        [future.result() for future in futures]


def async_ensure_reconcile(DB, logger, add_background_task):
    reconcile_marker_ref = DB.collection("global_reconcile_marker").document("marker")
    is_reconciling = reconcile_marker_ref.get().to_dict()["is_reconciling"]

    if not is_reconciling:
        add_background_task(reconcile, DB, logger, add_background_task)


def reconcile(db: firestore.Client, logger: Logger, add_background_task: Callable):
    """
    Modify cluster such that: current state -> correct/optimal state.
    Every cluster operation (adding/deleting/assigning nodes) has a non 100% chance of success.
    To make sure the cluster works when actions fail, we frequently check what the state
    should be then adjust things accordingly (what this function does).

    Below I remove nodes from the nodes list as they are addressed / fixed.
    """
    logger.log("Reconciling now.")
    instance_client = InstancesClient()

    # record globally that reconciling is happening, prevents simoultainous reconciling
    reconcile_marker_ref = db.collection("global_reconcile_marker").document("marker")
    reconcile_marker_ref.update({"is_reconciling": True})

    # load nodes from db
    nodes = []
    node_not_deleted = FieldFilter("status", "not-in", ["DELETED", "FAILED"])
    node_doc_snapshots = db.collection("nodes").where(filter=node_not_deleted).stream()
    for node_snapshot in node_doc_snapshots:
        node = Node.from_snapshot(db, logger, add_background_task, node_snapshot)
        nodes.append(node)

    # Get list of burla nodes from GCE
    nodes_from_gce = []
    for zone, instances_scope in instance_client.aggregated_list(project=PROJECT_ID):
        vms = getattr(instances_scope, "instances", [])
        burla_tag = "burla-cluster-node"
        gce_nodes = [GCEBurlaNode(i.name, zone, i.status) for i in vms if burla_tag in i.tags.items]
        nodes_from_gce.extend(gce_nodes)

    # 1. Delete nodes that are in GCE and not tracked in the DB.
    names_of_nodes_from_db = [node.instance_name for node in nodes]
    for gce_node in nodes_from_gce:
        deleting_or_starting_status = ["STOPPING", "TERMINATED", "PROVISIONING"]
        gce_node_not_deleting_or_starting = gce_node.status not in deleting_or_starting_status
        node_in_gce_and_not_in_db = gce_node.name not in names_of_nodes_from_db

        if node_in_gce_and_not_in_db and gce_node_not_deleting_or_starting:
            msg = f"Deleting node {gce_node.name} because it is NOT IN the DB!"
            logger.log(msg + f" (instance-status={gce_node.status})", severity="WARNING")
            try:
                zone = gce_node.zone.split("/")[1]
                add_background_task(
                    instance_client.delete,
                    project=PROJECT_ID,
                    zone=zone,
                    instance=gce_node.name,
                )
            except NotFound:
                pass

    # 2. Check that status of each node in db, ensure status makes sense, correct accordingly.
    names_of_nodes_from_gce = [gce_node.name for gce_node in nodes_from_gce]
    for node in nodes:
        node_not_in_gce = node.instance_name not in names_of_nodes_from_gce
        status = node.status()

        if status in ["READY", "RUNNING"] and node_not_in_gce:
            # node in database but not in compute engine?
            db.collection("nodes").document(node.instance_name).update({"status": "DELETED"})
        elif status == "BOOTING" and node_not_in_gce:
            # been booting for too long ?
            time_since_boot = time() - node.started_booting_at
            gce_vm_should_exist_by_now = time_since_boot > 45
            if gce_vm_should_exist_by_now:
                msg = f"Deleting node {node.instance_name} because it is not in GCE yet "
                msg += f"and it started booting {time_since_boot}s ago."
                logger.log(msg)
                node.delete()
                db.collection("nodes").document(node.instance_name).update({"status": "DELETED"})
                nodes.remove(node)
        elif status == "RUNNING":
            # job is still active?
            job_doc_ref = db.collection("jobs").document(node.current_job)
            job = job_doc_ref.get().to_dict()
            n_results = job_doc_ref.collection("results").count().get()[0][0].value
            job_ended = n_results == job["n_inputs"]
            if job_ended:
                msg = f"Rebooting node {node.instance_name}"
                logger.log(msg + f"because it's job ({node.current_job}) has ended.")
                node.async_reboot()
        elif status == "FAILED":
            # Delete node
            logger.log(f"Deleting node: {node.instance_name} because it has FAILED")
            node.delete()
            nodes.remove(node)

    # 3. Check that the cluster does or will match the specified default configuration.
    config = db.collection("cluster_config").document("cluster_config").get().to_dict()
    for spec in config["Nodes"]:
        # standby_nodes = [n for n in nodes if n.current_job is None]
        standby_nodes = [n for n in nodes if n.machine_type == spec["machine_type"]]

        # not enough of this machine_type on standby ? (add more standby nodes ?)
        if len(standby_nodes) < spec["quantity"]:
            node_deficit = spec["quantity"] - len(standby_nodes)
            for i in range(node_deficit):
                containers = [Container.from_dict(c) for c in spec["containers"]]
                machine = spec["machine_type"]
                logger.log(f"Adding another {machine} because cluster is {node_deficit-i} short.")

                def add_node(machine_type, containers, inactivity_shutdown_time_sec):
                    Node.start(
                        db=db,
                        logger=logger,
                        add_background_task=add_background_task,
                        machine_type=machine_type,
                        containers=containers,
                        inactivity_shutdown_time_sec=inactivity_shutdown_time_sec,
                        verbose=True,
                    )

                add_background_task(
                    add_node,
                    spec["machine_type"],
                    containers,
                    spec.get("inactivity_shutdown_time_sec"),
                )

        # too many of this machine_type on standby ?  (remove some standby nodes ?)
        elif len(standby_nodes) > spec["quantity"]:
            nodes_to_remove = sorted(standby_nodes, key=lambda n: n.time_until_booted())
            num_extra_nodes = len(standby_nodes) - spec["quantity"]
            nodes_to_remove = nodes_to_remove[-num_extra_nodes:]

            for i, node in enumerate(nodes_to_remove):
                surplus = len(nodes_to_remove) - i
                machine = spec["machine_type"]
                logger.log(f"DELETING an {machine} node because cluster has {surplus} too many")
                node.delete()

    # record globally that reconciling is done, prevents simoultainous reconciling
    reconcile_marker_ref = db.collection("global_reconcile_marker").document("marker")
    reconcile_marker_ref.update({"is_reconciling": False})
    logger.log("Done reconciling.")

    # 4. Check that none of the current jobs are done, failed, or not being worked on.
    #    (they should be marked as having ended)
    #
    # job_not_over = FieldFilter("ended_at", "==", None)
    # job_doc_refs = db.collection("jobs").where(filter=job_not_over).stream()
    # for job_ref in job_doc_refs:
    #     job_doc = job_ref.to_dict()
    #     job = Job(
    #         id=job_ref.id,
    #         current_parallelism=job_doc["current_parallelism"],
    #         target_parallelism=job_doc["target_parallelism"],
    #     )
    #     nodes_assigned_to_job = [node for node in nodes if node.current_job == job.id]
    #     nodes_working_on_job = [n for n in nodes_assigned_to_job if n.status() == "RUNNING"]

    #     if not nodes_assigned_to_job:
    #         # mark job as ended
    #         job_ref = db.collection("jobs").document(job.id)
    #         job_ref.update({"ended_at": time()})
    #     elif nodes_assigned_to_job and not nodes_working_on_job:
    #         # state of these nodes should be one of: please_reboot, booting, ready?
    #         # Nodes should have ultimatums, eg:
    #         #    Be in state X within Y amount of time or your state is set to: "FAILED"
    #         pass

    #     any_failed = False
    #     all_done = True
    #     for node in nodes_working_on_job:
    #         job_status = node.job_status(job_id=job.id)
    #         any_failed = job_status["any_subjobs_failed"]
    #         all_done = job_status["all_subjobs_done"]
    #         node_is_done = any_failed or job_status["all_subjobs_done"]

    #         if node_is_done and node.delete_when_done:
    #             node.delete()
    #             nodes.remove(node)
    #         elif node_is_done:
    #             add_background_task(reassign_or_remove_node, node)

    #     if any_failed or all_done:
    #         _remove_job_from_cluster_state_in_db(job.id)
    #     if any_failed:
    #         return "FAILED"
    #     if all_done:
    #         return "DONE"

    #     return "RUNNING"

    #
    # 5. Check that all jobs do or will match the target level of parallelism.
    #
