import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import storage


router = APIRouter()

BUCKET_NAME = "burla-test-shared-workspace"
FOLDER_PLACEHOLDER_NAME = ".burla-folder-placeholder"


def error_response(message: str, code: str = "400") -> Dict[str, Any]:
    return {"error": {"code": code, "message": message}}


def normalize_directory_path(raw_path: Optional[str]) -> str:
    if not raw_path:
        return ""
    normalized = raw_path.replace("\\", "/")
    segments: List[str] = []
    for segment in normalized.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if segments:
                segments.pop()
            continue
        segments.append(segment)
    if not segments:
        return ""
    return "/".join(segments) + "/"


def validate_entry_name(name: Optional[str]) -> str:
    if not name:
        raise ValueError("Name is required")
    if any(separator in name for separator in ("/", "\\")):
        raise ValueError("Name cannot contain path separators")
    if name in (".", ".."):
        raise ValueError("Name is not allowed")
    return name


def directory_path_for_response(directory_prefix: str) -> str:
    if not directory_prefix:
        return "/"
    return f"/{directory_prefix}"


def isoformat_value(timestamp: Optional[datetime.datetime]) -> str:
    if not timestamp:
        return ""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
    else:
        timestamp = timestamp.astimezone(datetime.timezone.utc)
    return timestamp.isoformat().replace("+00:00", "Z")


def folder_has_children(bucket: storage.Bucket, prefix: str) -> bool:
    iterator = bucket.list_blobs(prefix=prefix, max_results=2)
    for blob in iterator:
        if blob.name == prefix:
            continue
        if blob.name.endswith(FOLDER_PLACEHOLDER_NAME):
            continue
        return True
    return False


def build_directory_metadata(
    bucket: storage.Bucket, prefix: str, parent_prefix: str
) -> Dict[str, Any]:
    if prefix.endswith("/"):
        prefix = prefix
    else:
        prefix = f"{prefix}/"
    if parent_prefix:
        name = prefix[len(parent_prefix) : -1]
    else:
        name = prefix[:-1] or "/shared_workspace"
    directory_path = directory_path_for_response(parent_prefix)
    return {
        "name": name,
        "size": 0,
        "dateModified": "",
        "type": "folder",
        "isFile": False,
        "hasChild": folder_has_children(bucket, prefix),
        "path": directory_path,
        "filterPath": directory_path,
    }


def build_file_metadata(blob: storage.Blob, directory_prefix: str) -> Dict[str, Any]:
    name = blob.name[len(directory_prefix) :]
    directory_path = directory_path_for_response(directory_prefix)
    return {
        "name": name,
        "size": blob.size or 0,
        "dateModified": isoformat_value(blob.updated),
        "type": "file",
        "isFile": True,
        "hasChild": False,
        "path": directory_path,
        "filterPath": directory_path,
    }


def build_cwd_metadata(directory_prefix: str, has_children: bool) -> Dict[str, Any]:
    if not directory_prefix:
        name = "/shared_workspace"
    else:
        stripped = directory_prefix.rstrip("/")
        name = stripped.split("/")[-1]
    directory_path = directory_path_for_response(directory_prefix)
    return {
        "name": name,
        "size": 0,
        "dateModified": "",
        "type": "folder",
        "isFile": False,
        "hasChild": has_children,
        "path": directory_path,
        "filterPath": directory_path,
    }


def is_file_entry(entry: Dict[str, Any]) -> bool:
    if "isFile" in entry:
        return bool(entry["isFile"])
    if entry.get("type") == "folder":
        return False
    return True


def folder_exists(bucket: storage.Bucket, prefix: str) -> bool:
    placeholder_blob_name = f"{prefix}{FOLDER_PLACEHOLDER_NAME}"
    if bucket.get_blob(placeholder_blob_name):
        return True
    iterator = bucket.list_blobs(prefix=prefix, max_results=1)
    for blob in iterator:
        if blob.name == prefix:
            continue
        return True
    return False


def delete_prefix(bucket: storage.Bucket, prefix: str) -> None:
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        blob.delete()


def move_prefix(bucket: storage.Bucket, source_prefix: str, destination_prefix: str) -> None:
    blobs = list(bucket.list_blobs(prefix=source_prefix))
    for blob in blobs:
        destination_name = destination_prefix + blob.name[len(source_prefix) :]
        bucket.rename_blob(blob, destination_name)


def read_action(bucket: storage.Bucket, payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    iterator = bucket.list_blobs(prefix=directory_prefix, delimiter="/")
    directories: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []
    for page in iterator.pages:
        for prefix in getattr(page, "prefixes", []):
            directories.append(build_directory_metadata(bucket, prefix, directory_prefix))
        for blob in page:
            if blob.name == directory_prefix:
                continue
            if blob.name.endswith(FOLDER_PLACEHOLDER_NAME):
                continue
            if blob.name.endswith("/"):
                continue
            files.append(build_file_metadata(blob, directory_prefix))
    combined: List[Dict[str, Any]] = directories + files
    response = {
        "cwd": build_cwd_metadata(directory_prefix, bool(combined)),
        "files": combined,
    }
    return response


def create_action(bucket: storage.Bucket, payload: Dict[str, Any]) -> Dict[str, Any]:

    directory_prefix = normalize_directory_path(payload.get("path"))
    entries: List[Dict[str, Any]] = []

    payload_name = payload.get("name")
    candidate_names: List[str] = []
    if payload_name:
        candidate_names.append(payload_name)

    data_items = payload.get("data") or []
    if not candidate_names and not data_items:
        raise ValueError("No items provided for create action")

    for item in data_items:
        raw_name = item.get("name")
        if not raw_name:
            continue
        if "/" in raw_name or "\\" in raw_name:
            continue
        candidate_names.append(raw_name)

    unique_names = []
    seen = set()
    for raw_name in candidate_names:
        if raw_name in seen:
            continue
        seen.add(raw_name)
        unique_names.append(raw_name)

    if not unique_names:
        raise ValueError("No items provided for create action")

    for raw_name in unique_names:
        name = validate_entry_name(raw_name)
        folder_prefix = f"{directory_prefix}{name}/"
        file_blob = bucket.get_blob(f"{directory_prefix}{name}")
        if file_blob is not None:
            raise ValueError(f"A file named '{name}' already exists")
        if folder_exists(bucket, folder_prefix):
            metadata = build_directory_metadata(bucket, folder_prefix, directory_prefix)
            entries.append(metadata)
            continue
        placeholder_blob_name = f"{folder_prefix}{FOLDER_PLACEHOLDER_NAME}"
        if bucket.get_blob(placeholder_blob_name) is None:
            placeholder_blob = bucket.blob(placeholder_blob_name)
            placeholder_blob.upload_from_string("", content_type="application/octet-stream")
        metadata = build_directory_metadata(bucket, folder_prefix, directory_prefix)
        metadata["dateModified"] = isoformat_value(
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        )
        entries.append(metadata)
    return {"files": entries}


def delete_action(bucket: storage.Bucket, payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    data_items = payload.get("data") or []
    if not data_items:
        names = payload.get("names") or []
        data_items = [{"name": name} for name in names]
    if not data_items:
        raise ValueError("No items provided for delete action")
    for item in data_items:
        name = validate_entry_name(item.get("name"))
        if is_file_entry(item):
            blob_name = f"{directory_prefix}{name}"
            blob = bucket.blob(blob_name)
            try:
                blob.delete()
            except NotFound:
                continue
        else:
            folder_prefix = f"{directory_prefix}{name}/"
            delete_prefix(bucket, folder_prefix)
    return {"files": []}


def move_action(bucket: storage.Bucket, payload: Dict[str, Any]) -> Dict[str, Any]:
    source_directory_prefix = normalize_directory_path(payload.get("path"))
    target_path = payload.get("targetPath")
    if target_path is None:
        raise ValueError("Target path is required for move action")
    target_directory_prefix = normalize_directory_path(target_path)
    data_items = payload.get("data") or []
    if not data_items:
        names = payload.get("names") or []
        data_items = [{"name": name} for name in names]
    if not data_items:
        raise ValueError("No items provided for move action")
    entries: List[Dict[str, Any]] = []
    for item in data_items:
        name = validate_entry_name(item.get("name"))
        if is_file_entry(item):
            source_key = f"{source_directory_prefix}{name}"
            destination_key = f"{target_directory_prefix}{name}"
            if source_key == destination_key:
                existing_blob = bucket.get_blob(source_key)
                if not existing_blob:
                    raise NotFound(f"File '{name}' not found")
                entries.append(build_file_metadata(existing_blob, target_directory_prefix))
                continue
            if bucket.get_blob(destination_key):
                raise ValueError(f"A file named '{name}' already exists at the destination")
            blob = bucket.get_blob(source_key)
            if not blob:
                raise NotFound(f"File '{name}' not found")
            bucket.rename_blob(blob, destination_key)
            updated_blob = bucket.get_blob(destination_key)
            if not updated_blob:
                raise NotFound(f"File '{name}' not found after move")
            entries.append(build_file_metadata(updated_blob, target_directory_prefix))
        else:
            source_prefix = f"{source_directory_prefix}{name}/"
            destination_prefix = f"{target_directory_prefix}{name}/"
            if source_prefix == destination_prefix:
                metadata = build_directory_metadata(
                    bucket, destination_prefix, target_directory_prefix
                )
                metadata["dateModified"] = isoformat_value(
                    datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                )
                entries.append(metadata)
                continue
            if destination_prefix.startswith(source_prefix):
                raise ValueError("Cannot move a folder into itself or its subfolders")
            if folder_exists(bucket, destination_prefix):
                raise ValueError(f"A folder named '{name}' already exists at the destination")
            move_prefix(bucket, source_prefix, destination_prefix)
            metadata = build_directory_metadata(bucket, destination_prefix, target_directory_prefix)
            metadata["dateModified"] = isoformat_value(
                datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            )
            entries.append(metadata)
    return {"files": entries}


def rename_action(bucket: storage.Bucket, payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    data_items = payload.get("data") or []
    if not data_items:
        raise ValueError("No source item found for rename action")
    item = data_items[0]
    current_name = validate_entry_name(item.get("name"))
    new_name = validate_entry_name(payload.get("newName"))
    if current_name == new_name:
        raise ValueError("New name matches the existing name")
    if is_file_entry(item):
        source_key = f"{directory_prefix}{current_name}"
        destination_key = f"{directory_prefix}{new_name}"
        blob = bucket.get_blob(source_key)
        if not blob:
            raise NotFound(f"File '{current_name}' not found")
        bucket.rename_blob(blob, destination_key)
        updated_blob = bucket.get_blob(destination_key)
        if not updated_blob:
            raise NotFound(f"File '{new_name}' not found after rename")
        metadata = build_file_metadata(updated_blob, directory_prefix)
    else:
        source_prefix = f"{directory_prefix}{current_name}/"
        destination_prefix = f"{directory_prefix}{new_name}/"
        if folder_exists(bucket, destination_prefix):
            raise ValueError(f"Folder '{new_name}' already exists")
        move_prefix(bucket, source_prefix, destination_prefix)
        metadata = build_directory_metadata(bucket, destination_prefix, directory_prefix)
        metadata["dateModified"] = isoformat_value(
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        )
    return {"files": [metadata]}


def details_action(bucket: storage.Bucket, payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    names: List[str] = []
    data_items = payload.get("data") or []
    if data_items:
        names.extend(
            [validate_entry_name(item.get("name")) for item in data_items if item.get("name")]
        )
    request_names = payload.get("names") or []
    for raw_name in request_names:
        try:
            names.append(validate_entry_name(raw_name))
        except ValueError:
            continue
    if not names:
        return {"details": []}
    details: List[Dict[str, Any]] = []
    for name in names:
        file_blob = bucket.get_blob(f"{directory_prefix}{name}")
        if file_blob is not None:
            details.append(build_file_metadata(file_blob, directory_prefix))
            continue
        folder_prefix = f"{directory_prefix}{name}/"
        if folder_exists(bucket, folder_prefix):
            details.append(build_directory_metadata(bucket, folder_prefix, directory_prefix))
    return {"details": details}


@router.post("/api/sf/filemanager")
async def filemanager_endpoint(request: Request):
    request_json = await request.json()
    action = (request_json.get("action") or "").lower()
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    try:
        if action == "read":
            return read_action(bucket, request_json)
        if action == "create":
            return create_action(bucket, request_json)
        if action == "delete":
            return delete_action(bucket, request_json)
        if action == "move":
            return move_action(bucket, request_json)
        if action == "rename":
            return rename_action(bucket, request_json)
        if action == "details":
            return details_action(bucket, request_json)
        return error_response("Unsupported action", "400")
    except NotFound as not_found_error:
        return error_response(str(not_found_error), "404")
    except (GoogleAPIError, ValueError) as api_error:
        return error_response(str(api_error), "400")


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


@router.get("/signed-download")
def signed_download(
    name: str = Query(...),
    path: Optional[str] = Query("/"),
    expires_in_seconds: int = Query(900, ge=1, le=604800),
):
    directory_prefix = normalize_directory_path(path)
    entry_name = validate_entry_name(name)
    object_key = f"{directory_prefix}{entry_name}"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.get_blob(object_key)
    if blob is None:
        raise NotFound(f"File '{entry_name}' not found")
    expiration = datetime.timedelta(seconds=expires_in_seconds)
    safe_filename = entry_name.replace('"', '\\"')
    url = blob.generate_signed_url(
        version="v4",
        expiration=expiration,
        method="GET",
        response_disposition=f'attachment; filename="{safe_filename}"',
    )
    return {"url": url, "contentType": blob.content_type or "application/octet-stream"}
