# src/main_service/endpoints/settings.py

from fastapi import APIRouter, Depends, Request 
from fastapi.responses import JSONResponse
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import firestore

from main_service import DB, get_user_email, get_logger, get_request_json
from main_service.helpers import Logger
from typing import Dict

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
        user_emails = [user_doc.to_dict().get("email") for user_doc in users_docs if "email" in user_doc.to_dict()]

        return {
            "containerImage": container.get("image", ""),
            "pythonExecutable": container.get("python_executable", ""),
            "pythonVersion": container.get("python_version", ""),
            "machineType": node.get("machine_type", ""),
            "machineQuantity": node.get("quantity", 1),
            "users": user_emails  # Returning the list of user emails
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
        container["python_executable"] = request_json.get("pythonExecutable", container.get("python_executable"))
        container["python_version"] = request_json.get("pythonVersion", container.get("python_version"))
        
        # Update the node with the provided settings
        node["machine_type"] = request_json.get("machineType", node.get("machine_type"))
        node["quantity"] = request_json.get("machineQuantity", node.get("quantity"))

        # Reassign updated container and node data
        nodes[0]["containers"] = [container]  # Ensure containers array is updated
        
        # Update the "Nodes" field in Firestore with the modified node data
        doc_ref.set({"Nodes": nodes}, merge=True)

         # Fetch the current users from Firestore
        current_users = doc_ref.collection("Users").stream()
        print(current_users)
        # Get current user emails from the existing documents in the Users collection
        current_user_emails = {user_doc.to_dict().get("email"): user_doc.reference for user_doc in current_users}
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