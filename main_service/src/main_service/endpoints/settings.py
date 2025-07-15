import requests

from fastapi import APIRouter, Depends, Request

from main_service import DB, get_logger, PROJECT_ID, CLUSTER_ID_TOKEN
from main_service.helpers import Logger

router = APIRouter()
BURLA_BACKEND_URL = "https://backend.burla.dev"


@router.get("/v1/settings")
def get_settings(request: Request, logger: Logger = Depends(get_logger)):
    config_doc = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_doc.get().to_dict()
    node = config_dict.get("Nodes", [{}])[0]
    container = node.get("containers", [{}])[0]

    url = f"{BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}/users"
    response = requests.get(url, headers={"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"})
    response.raise_for_status()
    user_emails = [user["email"] for user in response.json()["authorized_users"]]
    return {
        "containerImage": container.get("image", ""),
        "pythonVersion": container.get("python_version", ""),
        "machineType": node.get("machine_type", ""),
        "gcpRegion": node.get("gcp_region", ""),
        "machineQuantity": node.get("quantity", 1),
        "diskSize": node.get("disk_size_gb", 50),
        "inactivityTimeout": int(node.get("inactivity_shutdown_time_sec", 600) // 60),
        "users": user_emails,
    }


@router.post("/v1/settings")
async def update_settings(request: Request, logger: Logger = Depends(get_logger)):
    request_json = await request.json()
    config_ref = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_ref.get().to_dict()

    # updates Nodes object in cluster_config doc
    nodes = config_dict.get("Nodes", [{}])
    node = nodes[0]
    container = node.get("containers", [{}])[0]
    container.update(
        {
            "image": request_json.get("containerImage", container.get("image")),
            "python_version": request_json.get("pythonVersion", container.get("python_version")),
        }
    )
    node.update(
        {
            "machine_type": request_json.get("machineType", node.get("machine_type")),
            "gcp_region": request_json.get("gcpRegion", node.get("gcp_region")),
            "quantity": request_json.get("machineQuantity", node.get("quantity")),
            "disk_size_gb": request_json.get("diskSize", node.get("disk_size_gb")),
            "inactivity_shutdown_time_sec": (
                request_json.get("inactivityTimeout", node.get("inactivity_shutdown_time_sec")) * 60
                if isinstance(request_json.get("inactivityTimeout"), int)
                else node.get("inactivity_shutdown_time_sec")
            ),
        }
    )
    nodes[0]["containers"] = [container]
    config_ref.update({"Nodes": nodes})

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    headers = {"Authorization": authorization, "X-User-Email": email}

    # updates users in backend service
    users_url = f"{BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}/users"
    response = requests.get(users_url, headers={"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"})
    response.raise_for_status()
    current_user_emails = [user["email"] for user in response.json()["authorized_users"]]
    new_user_emails = request_json.get("users", [])
    # add new users
    for email in new_user_emails:
        if email not in current_user_emails:
            response = requests.post(users_url, json={"new_user": email}, headers=headers)
            response.raise_for_status()
    # remove users
    for email in current_user_emails:
        if email not in new_user_emails:
            response = requests.delete(users_url, json={"user_to_remove": email}, headers=headers)
            response.raise_for_status()
