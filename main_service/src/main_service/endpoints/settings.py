import requests

from fastapi import APIRouter, HTTPException, Request

from main_service import (
    DB,
    PROJECT_ID,
    CLUSTER_ID_TOKEN,
    CURRENT_BURLA_VERSION,
    IN_LOCAL_DEV_MODE,
    LOCAL_DEV_CONFIG,
)

router = APIRouter()
BURLA_BACKEND_URL = "https://backend.burla.dev"


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_quota_limit(machine_type: str, gcp_region: str) -> int:
    quota_doc = DB.collection("cluster_quota").document("cluster_quota").get().to_dict() or {}
    region_quota = quota_doc.get(gcp_region, {})
    machine_type_limits = region_quota.get("machine_type_limits", {})
    return _safe_int(machine_type_limits.get(machine_type), 0)


def _quota_exceeded_message(machine_type: str, gcp_region: str, limit: int, requested: int) -> str:
    return (
        "Quota exceeded.\n"
        f"Limit for {machine_type} in {gcp_region} is {limit}.\n"
        f"Requested: {requested}.\n"
        "We're requesting a quota increase from GCP and will follow up shortly."
    )


@router.get("/v1/settings")
def get_settings(request: Request):
    config_doc = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_doc.get().to_dict()

    if IN_LOCAL_DEV_MODE:
        config_dict = LOCAL_DEV_CONFIG

    node = config_dict.get("Nodes", [{}])[0]
    container = node.get("containers", [{}])[0]

    url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
    response = requests.get(url, headers={"Authorization": f"Bearer {CLUSTER_ID_TOKEN}"})
    response.raise_for_status()
    user_emails = [user["email"] for user in response.json()["authorized_users"]]
    return {
        "containerImage": container.get("image", ""),
        "machineType": node.get("machine_type", ""),
        "gcpRegion": node.get("gcp_region", ""),
        "machineQuantity": node.get("quantity", 1),
        "diskSize": node.get("disk_size_gb", 50),
        "inactivityTimeout": int(node.get("inactivity_shutdown_time_sec", 600) // 60),
        "users": user_emails,
        "burlaVersion": CURRENT_BURLA_VERSION,
        "googleCloudProjectId": PROJECT_ID,
    }


@router.post("/v1/settings")
async def update_settings(request: Request):
    request_json = await request.json()
    config_ref = DB.collection("cluster_config").document("cluster_config")
    config_dict = config_ref.get().to_dict()

    # updates Nodes object in cluster_config doc
    nodes = config_dict.get("Nodes", [{}])
    node = nodes[0]
    container = node.get("containers", [{}])[0]

    requested_machine_type = request_json.get("machineType", node.get("machine_type"))
    requested_region = request_json.get("gcpRegion", node.get("gcp_region"))
    current_quantity = _safe_int(node.get("quantity"), 1)
    requested_quantity = _safe_int(
        request_json.get("machineQuantity", node.get("quantity")),
        current_quantity,
    )

    quota_limit = _get_quota_limit(requested_machine_type, requested_region)
    if requested_quantity > quota_limit:
        message = _quota_exceeded_message(
            machine_type=requested_machine_type,
            gcp_region=requested_region,
            limit=quota_limit,
            requested=requested_quantity,
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error_code": "quota_exceeded",
                "message": message,
                "machine_type": requested_machine_type,
                "region": requested_region,
                "limit": quota_limit,
                "requested": requested_quantity,
            },
        )

    container.update(
        {
            "image": request_json.get("containerImage", container.get("image")),
        }
    )
    container.pop("python_command", None)
    container.pop("python_version", None)
    node.update(
        {
            "machine_type": requested_machine_type,
            "gcp_region": requested_region,
            "quantity": requested_quantity,
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

    if IN_LOCAL_DEV_MODE:
        LOCAL_DEV_CONFIG["Nodes"] = nodes
        LOCAL_DEV_CONFIG["Nodes"][0]["machine_type"] = "n4-standard-2"
        LOCAL_DEV_CONFIG["Nodes"][0]["quantity"] = 1

    email = request.session.get("X-User-Email")
    authorization = request.session.get("Authorization")
    headers = {"Authorization": authorization, "X-User-Email": email}

    # updates users in backend service
    users_url = f"{BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
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
