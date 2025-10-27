import datetime
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.auth import default, impersonated_credentials

from main_service import PROJECT_ID, DB


router = APIRouter()

# This makes it possible to create signed url's for any blobs created with this client.
source_creds, project_id = default()
signing_creds = impersonated_credentials.Credentials(
    source_credentials=source_creds,
    target_principal=f"burla-main-service@{PROJECT_ID}.iam.gserviceaccount.com",
    target_scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
)
gcs_client = storage.Client(project=project_id, credentials=signing_creds)
cluster_config = DB.collection("cluster_config").document("cluster_config").get().to_dict()
GCS_BUCKET = gcs_client.bucket(cluster_config["gcs_bucket_name"])


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


def folder_has_children(prefix: str) -> bool:
    iterator = GCS_BUCKET.list_blobs(prefix=prefix, max_results=2)
    for blob in iterator:
        if blob.name == prefix:
            continue
        return True
    return False


def build_directory_metadata(prefix: str, parent_prefix: str) -> Dict[str, Any]:
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
        "hasChild": folder_has_children(prefix),
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


def folder_exists(prefix: str) -> bool:
    if GCS_BUCKET.get_blob(prefix):
        return True
    iterator = GCS_BUCKET.list_blobs(prefix=prefix, max_results=1)
    for blob in iterator:
        if blob.name == prefix:
            continue
        return True
    return False


def delete_prefix(prefix: str) -> None:
    blobs = GCS_BUCKET.list_blobs(prefix=prefix)
    for blob in blobs:
        blob.delete()


def move_prefix(source_prefix: str, destination_prefix: str) -> None:
    blobs = list(GCS_BUCKET.list_blobs(prefix=source_prefix))
    for blob in blobs:
        destination_name = destination_prefix + blob.name[len(source_prefix) :]
        GCS_BUCKET.rename_blob(blob, destination_name)


def read_action(payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    iterator = GCS_BUCKET.list_blobs(prefix=directory_prefix, delimiter="/")
    directories: List[Dict[str, Any]] = []
    files: List[Dict[str, Any]] = []
    for page in iterator.pages:
        for prefix in getattr(page, "prefixes", []):
            directories.append(build_directory_metadata(prefix, directory_prefix))
        for blob in page:
            if blob.name == directory_prefix:
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


def create_action(payload: Dict[str, Any]) -> Dict[str, Any]:
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
        file_blob = GCS_BUCKET.get_blob(f"{directory_prefix}{name}")
        if file_blob is not None:
            raise ValueError(f"A file named '{name}' already exists")
        if folder_exists(folder_prefix):
            metadata = build_directory_metadata(folder_prefix, directory_prefix)
            entries.append(metadata)
            continue
        if GCS_BUCKET.get_blob(folder_prefix) is None:
            marker_blob = GCS_BUCKET.blob(folder_prefix)
            marker_blob.upload_from_string(b"", content_type="application/x-directory")
        metadata = build_directory_metadata(folder_prefix, directory_prefix)
        metadata["dateModified"] = isoformat_value(
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        )
        entries.append(metadata)
    return {"files": entries}


def delete_action(payload: Dict[str, Any]) -> Dict[str, Any]:
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
            blob = GCS_BUCKET.blob(blob_name)
            try:
                blob.delete()
            except NotFound:
                continue
        else:
            folder_prefix = f"{directory_prefix}{name}/"
            delete_prefix(folder_prefix)
    return {"files": []}


def move_action(payload: Dict[str, Any]) -> Dict[str, Any]:
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
                existing_blob = GCS_BUCKET.get_blob(source_key)
                if not existing_blob:
                    raise NotFound(f"File '{name}' not found")
                entries.append(build_file_metadata(existing_blob, target_directory_prefix))
                continue
            if GCS_BUCKET.get_blob(destination_key):
                raise ValueError(f"A file named '{name}' already exists at the destination")
            blob = GCS_BUCKET.get_blob(source_key)
            if not blob:
                raise NotFound(f"File '{name}' not found")
            GCS_BUCKET.rename_blob(blob, destination_key)
            updated_blob = GCS_BUCKET.get_blob(destination_key)
            if not updated_blob:
                raise NotFound(f"File '{name}' not found after move")
            entries.append(build_file_metadata(updated_blob, target_directory_prefix))
        else:
            source_prefix = f"{source_directory_prefix}{name}/"
            destination_prefix = f"{target_directory_prefix}{name}/"
            if source_prefix == destination_prefix:
                metadata = build_directory_metadata(destination_prefix, target_directory_prefix)
                metadata["dateModified"] = isoformat_value(
                    datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                )
                entries.append(metadata)
                continue
            if destination_prefix.startswith(source_prefix):
                raise ValueError("Cannot move a folder into itself or its subfolders")
            if folder_exists(destination_prefix):
                raise ValueError(f"A folder named '{name}' already exists at the destination")
            move_prefix(source_prefix, destination_prefix)
            metadata = build_directory_metadata(destination_prefix, target_directory_prefix)
            metadata["dateModified"] = isoformat_value(
                datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            )
            entries.append(metadata)
    return {"files": entries}


def rename_action(payload: Dict[str, Any]) -> Dict[str, Any]:
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
        blob = GCS_BUCKET.get_blob(source_key)
        if not blob:
            raise NotFound(f"File '{current_name}' not found")
        GCS_BUCKET.rename_blob(blob, destination_key)
        updated_blob = GCS_BUCKET.get_blob(destination_key)
        if not updated_blob:
            raise NotFound(f"File '{new_name}' not found after rename")
        metadata = build_file_metadata(updated_blob, directory_prefix)
    else:
        source_prefix = f"{directory_prefix}{current_name}/"
        destination_prefix = f"{directory_prefix}{new_name}/"
        if folder_exists(destination_prefix):
            raise ValueError(f"Folder '{new_name}' already exists")
        move_prefix(source_prefix, destination_prefix)
        metadata = build_directory_metadata(destination_prefix, directory_prefix)
        metadata["dateModified"] = isoformat_value(
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        )
    return {"files": [metadata]}


def details_action(payload: Dict[str, Any]) -> Dict[str, Any]:
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
        file_blob = GCS_BUCKET.get_blob(f"{directory_prefix}{name}")
        if file_blob is not None:
            details.append(build_file_metadata(file_blob, directory_prefix))
            continue
        folder_prefix = f"{directory_prefix}{name}/"
        if folder_exists(folder_prefix):
            details.append(build_directory_metadata(folder_prefix, directory_prefix))
    return {"details": details}


@router.post("/api/sf/filemanager")
async def filemanager_endpoint(request: Request):
    request_json = await request.json()
    action = (request_json.get("action") or "").lower()
    try:
        if action == "read":
            return read_action(request_json)
        if action == "create":
            return create_action(request_json)
        if action == "delete":
            return delete_action(request_json)
        if action == "move":
            return move_action(request_json)
        if action == "rename":
            return rename_action(request_json)
        if action == "details":
            return details_action(request_json)
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
    blob = GCS_BUCKET.blob(object_name)
    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(days=7),
        method="POST",
        service_account_email=f"burla-main-service@{PROJECT_ID}.iam.gserviceaccount.com",
        content_type=content_type,
        headers={"x-goog-resumable": "start"},
    )
    return {"url": url}


def sanitize_object_name(raw_name: str) -> str:
    normalized = raw_name.lstrip("/")
    if normalized.endswith("/"):
        raise HTTPException(status_code=400, detail="Cannot download a folder path")
    if not normalized:
        raise HTTPException(status_code=400, detail="Object name is required")
    segments = []
    for segment in normalized.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            raise HTTPException(status_code=400, detail="Invalid object path")
        segments.append(segment)
    if not segments:
        raise HTTPException(status_code=400, detail="Object name is required")
    return "/".join(segments)


def sanitize_archive_filename(raw_name: Optional[str]) -> str:
    candidate = (raw_name or "").strip()
    if not candidate:
        candidate = "files.zip"
    sanitized = Path(candidate).name or "files.zip"
    if not sanitized.lower().endswith(".zip"):
        sanitized = f"{sanitized}.zip"
    return sanitized.replace('"', "").replace("'", "")


def sanitize_archive_item_path(raw_path: Optional[str], fallback: str) -> str:
    candidate = (raw_path or "").strip()
    selected = candidate or fallback
    normalized = selected.replace("\\", "/")
    segments: List[str] = []
    for segment in normalized.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            raise HTTPException(status_code=400, detail="Invalid archive path")
        segments.append(segment)
    if not segments:
        raise HTTPException(status_code=400, detail="Invalid archive path")
    return "/".join(segments)


def sanitize_folder_prefix(raw_prefix: Optional[str]) -> str:
    if not isinstance(raw_prefix, str):
        raise HTTPException(status_code=400, detail="Folder path is required")
    normalized = raw_prefix.strip().replace("\\", "/")
    segments: List[str] = []
    for segment in normalized.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            raise HTTPException(status_code=400, detail="Invalid folder path")
        segments.append(segment)
    if not segments:
        raise HTTPException(status_code=400, detail="Folder path is required")
    return "/".join(segments) + "/"


@router.get("/signed-download")
def signed_download(object_name: str = Query(...), download_name: Optional[str] = Query(None)):
    sanitized_object_name = sanitize_object_name(object_name)
    blob = GCS_BUCKET.blob(sanitized_object_name)
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"File '{sanitized_object_name}' not found")
    fallback_name = sanitized_object_name.split("/")[-1] or "download"
    safe_download_name = (download_name or fallback_name).replace('"', "").replace("'", "")
    disposition = f'attachment; filename="{safe_download_name}"'
    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(days=7),
        method="GET",
        response_disposition=disposition,
    )
    return {"url": url}


@router.post("/batch-download")
def batch_download(payload: Dict[str, Any]):
    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        raise HTTPException(status_code=400, detail="No files provided for download")
    file_requests: List[Dict[str, str]] = []
    folder_requests: List[Dict[str, str]] = []
    file_seen: set[str] = set()
    folder_seen: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        item_type = raw_item.get("type")
        if item_type == "folder":
            folder_prefix = raw_item.get("prefix")
            archive_root_name = raw_item.get("archivePath") or raw_item.get("name")
            if isinstance(archive_root_name, str):
                archive_root_name = archive_root_name.strip()
            sanitized_prefix = sanitize_folder_prefix(folder_prefix)
            if sanitized_prefix in folder_seen:
                continue
            try:
                archive_root = validate_entry_name(archive_root_name)
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error
            folder_seen.add(sanitized_prefix)
            folder_requests.append(
                {
                    "prefix": sanitized_prefix,
                    "archive_root": archive_root,
                }
            )
            continue
        object_name = raw_item.get("objectName")
        if not isinstance(object_name, str):
            continue
        sanitized_object_name = sanitize_object_name(object_name)
        if sanitized_object_name in file_seen:
            continue
        archive_path_value = raw_item.get("archivePath")
        archive_path = sanitize_archive_item_path(
            archive_path_value if isinstance(archive_path_value, str) else None,
            sanitized_object_name,
        )
        file_seen.add(sanitized_object_name)
        file_requests.append(
            {
                "object_name": sanitized_object_name,
                "archive_path": archive_path,
            }
        )
    if not file_requests and not folder_requests:
        raise HTTPException(status_code=400, detail="No files provided for download")
    archive_name = sanitize_archive_filename(payload.get("archiveName"))
    temp_file = tempfile.SpooledTemporaryFile(max_size=268_435_456)
    try:
        with zipfile.ZipFile(temp_file, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            written_entries: set[str] = set()

            def archive_path_for(root: str, relative_path: str) -> str:
                if not root:
                    return relative_path
                if not relative_path:
                    return root
                return f"{root}/{relative_path}"

            def ensure_directory_entry(path: str) -> None:
                normalized = path.strip("/")
                if not normalized:
                    return
                entry_name = f"{normalized}/"
                if entry_name in written_entries:
                    return
                archive.writestr(entry_name, "")
                written_entries.add(entry_name)

            def ensure_directory_hierarchy(root: str, directory_path: str) -> None:
                segments = [segment for segment in directory_path.split("/") if segment]
                if not segments:
                    ensure_directory_entry(root)
                    return
                accumulated: List[str] = []
                for segment in segments:
                    accumulated.append(segment)
                    ensure_directory_entry(archive_path_for(root, "/".join(accumulated)))

            def ensure_parent_directories(root: str, relative_file_path: str) -> None:
                parents = relative_file_path.split("/")[:-1]
                if not parents:
                    return
                ensure_directory_hierarchy(root, "/".join(parents))

            for request in file_requests:
                blob = GCS_BUCKET.get_blob(request["object_name"])
                if blob is None:
                    raise HTTPException(
                        status_code=404, detail=f"File '{request['object_name']}' not found"
                    )
                archive_path = request["archive_path"]
                if archive_path in written_entries:
                    continue
                ensure_parent_directories("", archive_path)
                with blob.open("rb") as source, archive.open(archive_path, "w") as target:
                    while True:
                        chunk = source.read(1024 * 1024)
                        if not chunk:
                            break
                        target.write(chunk)
                written_entries.add(archive_path)
            for request in folder_requests:
                prefix = request["prefix"]
                archive_root = request["archive_root"]
                ensure_directory_entry(archive_root)
                added_content = False
                blobs = GCS_BUCKET.list_blobs(prefix=prefix)
                for blob in blobs:
                    if blob.name == prefix:
                        continue
                    relative_path = blob.name[len(prefix) :]
                    if not relative_path:
                        continue
                    sanitized_relative = sanitize_archive_item_path(relative_path, relative_path)
                    archive_path = archive_path_for(archive_root, sanitized_relative)
                    if archive_path in written_entries:
                        continue
                    ensure_parent_directories(archive_root, sanitized_relative)
                    with blob.open("rb") as source, archive.open(archive_path, "w") as target:
                        while True:
                            chunk = source.read(1024 * 1024)
                            if not chunk:
                                break
                            target.write(chunk)
                    written_entries.add(archive_path)
                    added_content = True
                if not added_content:
                    ensure_directory_entry(archive_root)
    except HTTPException:
        temp_file.close()
        raise
    except NotFound as error:
        temp_file.close()
        raise HTTPException(status_code=404, detail=str(error)) from error
    except GoogleAPIError as error:
        temp_file.close()
        raise HTTPException(status_code=400, detail=str(error)) from error
    except Exception:
        temp_file.close()
        raise
    temp_file.seek(0, 2)
    total_size = temp_file.tell()
    temp_file.seek(0)

    def iterator():
        try:
            while True:
                chunk = temp_file.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            temp_file.close()

    headers = {"Content-Disposition": f'attachment; filename="{archive_name}"'}
    if total_size:
        headers["Content-Length"] = str(total_size)
    return StreamingResponse(iterator(), media_type="application/zip", headers=headers)
