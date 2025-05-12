# src/main_service/endpoints/settings.py

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import JSONResponse
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import firestore

from main_service import DB, get_user_email, get_logger, get_request_json
from main_service.helpers import Logger
from typing import Dict
import random
from uuid import uuid4
import string

router = APIRouter()

CLUSTER_CONFIG_DOC = "cluster_config"
CLUSTER_CONFIG_COLLECTION = "cluster_config"


@router.get("/v1/settings")
def get_settings(logger: Logger = Depends(get_logger)):
    try:
        # Fetch the cluster config document
        doc_ref = DB.collection(CLUSTER_CONFIG_COLLECTION).document(CLUSTER_CONFIG_DOC)
        doc = doc_ref.get()

        if not doc.exists:
            return JSONResponse(status_code=404, content={"error": "Cluster config not found."})

        # Extract the cluster configuration data
        doc_data = doc.to_dict()
        node = doc_data.get("Nodes", [{}])[0]
        container = node.get("containers", [{}])[0]

        # Get user emails from the 'users' subcollection
        users_ref = doc_ref.collection("Users")  # Access the subcollection 'users'
        users_docs = users_ref.stream()  # Get all documents in the 'users' subcollection

        # Extract emails
        user_emails = [
            user_doc.to_dict().get("email")
            for user_doc in users_docs
            if "email" in user_doc.to_dict()
        ]

        return {
            "containerImage": container.get("image", ""),
            "pythonVersion": container.get("python_version", ""),
            "machineType": node.get("machine_type", ""),
            "machineQuantity": node.get("quantity", 1),
            "users": user_emails,  # Returning the list of user emails
        }

    except GoogleAPICallError as e:
        logger.log(f"Error fetching settings: {e}", severity="ERROR")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@router.post("/v1/settings")
async def update_settings(request: Request, logger: Logger = Depends(get_logger)):
    try:
        # Extract the email from the request headers
        user_email = request.headers.get("Email")

        # Check if email is provided
        if not user_email:
            return JSONResponse(status_code=400, content={"error": "Email header is missing"})

        # Set the email in the request state
        request.state.user_email = user_email

        # Get the request JSON data
        request_json = await request.json()  # Now we define request_json

        # Fetch the cluster config document
        doc_ref = DB.collection(CLUSTER_CONFIG_COLLECTION).document(CLUSTER_CONFIG_DOC)
        doc = doc_ref.get()

        if not doc.exists:
            return JSONResponse(status_code=404, content={"error": "Cluster config not found."})

        # Extract the cluster configuration data
        doc_data = doc.to_dict()
        nodes = doc_data.get("Nodes", [{}])  # Fetch all nodes (default empty list if none)
        node = nodes[0]  # Assuming there's at least one node

        # Get the container from the first node (assuming there's at least one container)
        container = node.get("containers", [{}])[0]

        # Update the container with the provided settings
        container["image"] = request_json.get("containerImage", container.get("image"))
        container["python_version"] = request_json.get(
            "pythonVersion", container.get("python_version")
        )

        # Update the node with the provided settings
        node["machine_type"] = request_json.get("machineType", node.get("machine_type"))
        node["quantity"] = request_json.get("machineQuantity", node.get("quantity"))

        # Reassign updated container and node data
        nodes[0]["containers"] = [container]  # Ensure containers array is updated

        # Update the "Nodes" field in Firestore with the modified node data
        doc_ref.set({"Nodes": nodes}, merge=True)

        # Fetch the current users from Firestore
        current_users = doc_ref.collection("Users").stream()
        # Get current user emails from the existing documents in the Users collection
        current_user_emails = {
            user_doc.to_dict().get("email"): user_doc.reference for user_doc in current_users
        }
        print(current_user_emails)

        # Add new users (check if they already exist based on email)
        for email in request_json.get("users", []):
            if email not in current_user_emails:
                # Add a new document for the user with their email
                doc_ref.collection("Users").add({"email": email})

        # Remove users who are no longer part of the list
        for email, user_ref in current_user_emails.items():
            if email not in request_json.get("users", []):
                # Delete the user document if it doesn't exist in the new list
                user_ref.delete()

        return {"message": "Cluster config and user emails updated successfully."}

    except GoogleAPICallError as e:
        logger.log(f"Error updating cluster config: {e}", severity="ERROR")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


SERVICE_ACCOUNTS_COLLECTION = "service_accounts"

# Word banks for name generation
animals = ["panther", "otter", "lynx", "dolphin", "eagle"]
colors = ["blue", "scarlet", "ivory", "amber", "emerald"]
adjectives = ["brave", "sneaky", "quiet", "curious", "mighty"]


def pick(lst):
    return random.choice(lst)


def generate_name():
    return f"{pick(animals)}-{pick(colors)}-{pick(adjectives)}"


def generate_token():
    return "-".join(
        "".join(random.choices(string.ascii_lowercase + string.digits, k=8)) for _ in range(5)
    )


@router.post("/v1/service-accounts")
async def create_service_account(logger: Logger = Depends(get_logger)):
    try:
        # Generate unique ID, name, and token
        service_id = str(uuid4())
        name = generate_name()
        token = generate_token()

        # Create document in Firestore
        doc_ref = DB.collection(SERVICE_ACCOUNTS_COLLECTION).document(service_id)
        doc_ref.set(
            {
                "id": service_id,
                "name": name,
                "token": token,
            }
        )

        return {
            "id": service_id,
            "name": name,
            "token": token,
        }

    except Exception as e:
        logger.log(f"Error creating service account: {e}", severity="ERROR")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@router.delete("/v1/service-accounts/{service_id}")
async def delete_service_account(service_id: str, logger: Logger = Depends(get_logger)):
    try:
        doc_ref = DB.collection(SERVICE_ACCOUNTS_COLLECTION).document(service_id)
        if not doc_ref.get().exists:
            raise HTTPException(status_code=404, detail="Service account not found")

        doc_ref.delete()
        return {"message": f"Service account {service_id} deleted successfully"}

    except Exception as e:
        logger.log(f"Error deleting service account {service_id}: {e}", severity="ERROR")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@router.get("/v1/service-accounts")
async def list_service_accounts(logger: Logger = Depends(get_logger)):
    try:
        docs = DB.collection(SERVICE_ACCOUNTS_COLLECTION).stream()
        accounts = [doc.to_dict() for doc in docs]
        return {"service_accounts": accounts}

    except Exception as e:
        logger.log(f"Error listing service accounts: {e}", severity="ERROR")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})


@router.post("/v1/service-accounts/{service_id}/refresh-token")
async def refresh_service_account_token(service_id: str, logger: Logger = Depends(get_logger)):
    try:
        doc_ref = DB.collection(SERVICE_ACCOUNTS_COLLECTION).document(service_id)
        doc = doc_ref.get()

        if not doc.exists:
            raise HTTPException(status_code=404, detail="Service account not found")

        new_token = generate_token()
        doc_ref.update({"token": new_token})

        return {"token": new_token}

    except Exception as e:
        logger.log(f"Error refreshing token for {service_id}: {e}", severity="ERROR")
        return JSONResponse(status_code=500, content={"error": "Internal Server Error"})
