import datetime

from fastapi import APIRouter, Query, Request
from google.cloud import storage


router = APIRouter()

BUCKET_NAME = "burla-test-shared-workspace"


@router.post("/api/sf/filemanager")
async def filemanager_endpoint(request: Request):
    # This only satisfies Syncfusion's initial "read" so the UI renders.
    request_json = await request.json()
    if request_json.get("action") != "read":
        # Tell the UI nothing else is supported yet.
        return {
            "cwd": {"name": "", "size": 0, "dateModified": "", "type": "folder", "hasChild": False},
            "files": [],
        }

    # Minimal empty root listing
    return {
        "cwd": {"name": "", "size": 0, "dateModified": "", "type": "folder", "hasChild": False},
        "files": [],
    }


@router.post("/api/sf/upload")
async def upload_stub():
    return {"error": None}


@router.get("/signed-resumable")
def signed_resumable(
    object_name: str = Query(...), content_type: str = Query("application/octet-stream")
):
    client = storage.Client()
    blob = client.bucket(BUCKET_NAME).blob(object_name)
    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(days=7),
        method="POST",
        content_type=content_type,
        headers={"x-goog-resumable": "start"},
    )
    return {"url": url}
