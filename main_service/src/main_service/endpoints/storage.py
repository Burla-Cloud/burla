import datetime
import queue
import secrets
import threading
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from main_service import CLOUD_PROVIDER, get_add_background_task_function, history
from main_service.blobstore import BlobNotFound, get_blob_store

router = APIRouter()

_cluster_config = history.get_cluster_config()
STORE = get_blob_store(_cluster_config["gcs_bucket_name"])

BATCH_DELETE_BACKGROUND_THRESHOLD = 1000
BATCH_DELETE_CHUNK_SIZE = 1000
BATCH_DOWNLOAD_TOKEN_TTL_SECONDS = 300
BATCH_DOWNLOAD_TOKENS: Dict[str, Dict[str, Any]] = {}
BATCH_DOWNLOAD_TOKENS_LOCK = threading.Lock()

S3_UPLOAD_SESSION_TTL_SECONDS = 24 * 3600
S3_UPLOAD_SESSIONS: Dict[str, Dict[str, Any]] = {}
S3_UPLOAD_SESSIONS_LOCK = threading.Lock()


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


def build_directory_metadata(
    prefix: str, parent_prefix: str, has_children: bool = True
) -> Dict[str, Any]:
    if prefix.endswith("/"):
        prefix = prefix
    else:
        prefix = f"{prefix}/"
    if parent_prefix:
        name = prefix[len(parent_prefix) : -1]
    else:
        name = prefix[:-1] or "/workspace/shared"
    directory_path = directory_path_for_response(parent_prefix)
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


def build_file_metadata(blob_meta: dict, directory_prefix: str) -> Dict[str, Any]:
    name = blob_meta["name"][len(directory_prefix) :]
    directory_path = directory_path_for_response(directory_prefix)
    return {
        "name": name,
        "size": blob_meta["size"] or 0,
        "dateModified": isoformat_value(blob_meta["updated"]),
        "type": "file",
        "isFile": True,
        "hasChild": False,
        "path": directory_path,
        "filterPath": directory_path,
    }


def build_cwd_metadata(directory_prefix: str, has_children: bool) -> Dict[str, Any]:
    if not directory_prefix:
        name = "/workspace/shared"
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
    if STORE.get_blob_metadata(prefix):
        return True
    for blob_meta in STORE.list_prefix(prefix, max_results=2):
        if blob_meta["name"] == prefix:
            continue
        return True
    return False


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def collect_blob_names(
    directory_prefix: str, items: List[Dict[str, Any]], limit: Optional[int] = None
) -> tuple[List[str], bool]:
    names: List[str] = []
    seen: set[str] = set()
    truncated = False

    for item in items:
        entry_name = item["name"]
        entry_is_file = item["isFile"]

        if entry_is_file:
            blob_name = f"{directory_prefix}{entry_name}"
            if blob_name not in seen:
                seen.add(blob_name)
                names.append(blob_name)
        else:
            prefix = f"{directory_prefix}{entry_name}/"
            for blob_meta in STORE.list_prefix(prefix):
                if blob_meta["name"] in seen:
                    continue
                seen.add(blob_meta["name"])
                names.append(blob_meta["name"])
                if limit is not None and len(names) >= limit:
                    truncated = True
                    return names, truncated

        if limit is not None and len(names) >= limit:
            truncated = True
            return names, truncated

    return names, truncated


def delete_blobs_in_batches(blob_names: List[str]) -> None:
    if not blob_names:
        return

    for group in chunked(blob_names, BATCH_DELETE_CHUNK_SIZE):
        STORE.delete_batch(group)


def schedule_background_delete(directory_prefix: str, items: List[Dict[str, Any]]) -> None:
    blob_names, _ = collect_blob_names(directory_prefix, items)
    delete_blobs_in_batches(blob_names)


def move_prefix(source_prefix: str, destination_prefix: str) -> None:
    blob_metas = list(STORE.list_prefix(source_prefix))
    for blob_meta in blob_metas:
        destination_name = destination_prefix + blob_meta["name"][len(source_prefix) :]
        STORE.rename(blob_meta["name"], destination_name)


def extract_paging(payload: Dict[str, Any]) -> tuple[int, int]:
    """Read skip / take from payload and payload['data'] safely."""
    raw_skip = payload.get("skip")
    raw_take = payload.get("take")

    data = payload.get("data")
    if isinstance(data, dict):
        if raw_skip is None:
            raw_skip = data.get("skip")
        if raw_take is None:
            raw_take = data.get("take")

    def to_int(value: Any, default: int) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    skip = max(to_int(raw_skip, 0), 0)
    take = to_int(raw_take, 1000)

    if take <= 0:
        take = 1000
    if take > 5000:
        take = 5000

    return skip, take


def get_directory_page(
    directory_prefix: str,
    skip: int,
    take: int,
) -> tuple[List[Dict[str, Any]], int, bool]:
    needed = skip + take + 1

    entries: List[Dict[str, Any]] = []
    seen = 0
    remaining_needed = skip + take
    has_more = False

    for prefixes, file_metas in STORE.iter_pages(directory_prefix, max_results=max(needed, 1)):
        # folders
        for prefix in prefixes:
            if seen >= remaining_needed:
                has_more = True
                break

            if seen >= skip:
                entries.append(build_directory_metadata(prefix, directory_prefix, True))

            seen += 1

        if has_more:
            break

        # files
        for blob_meta in file_metas:
            if blob_meta["name"] == directory_prefix or blob_meta["name"].endswith("/"):
                continue

            if seen >= remaining_needed:
                has_more = True
                break

            if seen >= skip:
                entries.append(build_file_metadata(blob_meta, directory_prefix))

            seen += 1

        if has_more:
            break

    if has_more:
        fake_total = skip + len(entries) + 1
    else:
        fake_total = seen

    return entries, fake_total, has_more


def read_action(payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    skip, take = extract_paging(payload)

    files, total_count, has_more = get_directory_page(directory_prefix, skip, take)

    return {
        "cwd": build_cwd_metadata(directory_prefix, total_count > 0),
        "files": files,
        "count": total_count,
        "hasMore": has_more,
    }


def create_action(payload: Dict[str, Any]) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    entries: List[Dict[str, Any]] = []

    payload_name = payload.get("name")
    candidate_names: List[str] = []
    if payload_name:
        candidate_names.append(payload_name)
    else:
        data_items = payload.get("data") or []
        if not data_items:
            raise ValueError("No items provided for create action")
        for item in data_items:
            raw_name = item.get("name")
            if not raw_name:
                continue
            if "/" in raw_name or "\\" in raw_name:
                continue
            candidate_names.append(raw_name)

    if not candidate_names:
        raise ValueError("No items provided for create action")

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
        if STORE.get_blob_metadata(f"{directory_prefix}{name}") is not None:
            raise ValueError(f"A file named '{name}' already exists")
        if folder_exists(folder_prefix):
            metadata = build_directory_metadata(folder_prefix, directory_prefix, True)
            entries.append(metadata)
            continue
        if STORE.get_blob_metadata(folder_prefix) is None:
            STORE.upload_empty(folder_prefix, content_type="application/x-directory")
        metadata = build_directory_metadata(folder_prefix, directory_prefix, False)
        metadata["dateModified"] = isoformat_value(
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        )
        entries.append(metadata)
    return {"files": entries}


def delete_action(
    payload: Dict[str, Any], add_background_task: Optional[Callable] = None
) -> Dict[str, Any]:
    directory_prefix = normalize_directory_path(payload.get("path"))
    data_items = payload.get("data") or []
    if not data_items:
        names = payload.get("names") or []
        data_items = [{"name": name} for name in names]
    if not data_items:
        raise ValueError("No items provided for delete action")
    normalized_items: List[Dict[str, Any]] = []
    for item in data_items:
        name = validate_entry_name(item.get("name"))
        normalized_items.append({"name": name, "isFile": is_file_entry(item)})

    preview_names, truncated = collect_blob_names(
        directory_prefix, normalized_items, BATCH_DELETE_BACKGROUND_THRESHOLD + 1
    )

    over_background_threshold = truncated or len(preview_names) > BATCH_DELETE_BACKGROUND_THRESHOLD

    if over_background_threshold and add_background_task is not None:
        add_background_task(schedule_background_delete, directory_prefix, normalized_items)
        estimated_count = (
            len(preview_names) if not truncated else BATCH_DELETE_BACKGROUND_THRESHOLD + 1
        )
        return {
            "files": [],
            "backgroundDelete": True,
            "deletedCount": estimated_count,
        }

    full_names, _ = collect_blob_names(directory_prefix, normalized_items)
    delete_blobs_in_batches(full_names)
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
                existing = STORE.get_blob_metadata(source_key)
                if not existing:
                    raise BlobNotFound(f"File '{name}' not found")
                entries.append(build_file_metadata(existing, target_directory_prefix))
                continue
            if STORE.get_blob_metadata(destination_key):
                raise ValueError(f"A file named '{name}' already exists at the destination")
            STORE.rename(source_key, destination_key)
            updated = STORE.get_blob_metadata(destination_key)
            if not updated:
                raise BlobNotFound(f"File '{name}' not found after move")
            entries.append(build_file_metadata(updated, target_directory_prefix))
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
            metadata = build_directory_metadata(destination_prefix, target_directory_prefix, True)
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
        STORE.rename(source_key, destination_key)
        updated = STORE.get_blob_metadata(destination_key)
        if not updated:
            raise BlobNotFound(f"File '{new_name}' not found after rename")
        metadata = build_file_metadata(updated, directory_prefix)
    else:
        source_prefix = f"{directory_prefix}{current_name}/"
        destination_prefix = f"{directory_prefix}{new_name}/"
        if folder_exists(destination_prefix):
            raise ValueError(f"Folder '{new_name}' already exists")
        move_prefix(source_prefix, destination_prefix)
        metadata = build_directory_metadata(destination_prefix, directory_prefix, True)
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
        file_meta = STORE.get_blob_metadata(f"{directory_prefix}{name}")
        if file_meta is not None:
            details.append(build_file_metadata(file_meta, directory_prefix))
            continue
        folder_prefix = f"{directory_prefix}{name}/"
        if folder_exists(folder_prefix):
            details.append(build_directory_metadata(folder_prefix, directory_prefix))
    return {"details": details}


@router.post("/api/sf/filemanager")
async def filemanager_endpoint(
    request: Request, add_background_task=Depends(get_add_background_task_function)
):
    request_json = await request.json()
    action = (request_json.get("action") or "").lower()
    try:
        if action == "read":
            return read_action(request_json)
        if action == "create":
            return create_action(request_json)
        if action == "delete":
            return delete_action(request_json, add_background_task)
        if action == "move":
            return move_action(request_json)
        if action == "rename":
            return rename_action(request_json)
        if action == "details":
            return details_action(request_json)
        return error_response("Unsupported action", "400")
    except BlobNotFound as not_found_error:
        return error_response(str(not_found_error), "404")
    except ValueError as api_error:
        return error_response(str(api_error), "400")


@router.post("/api/sf/upload")
async def upload_stub():
    return {"error": None}


@router.get("/signed-resumable")
def signed_resumable(
    object_name: str = Query(...), content_type: str = Query("application/octet-stream")
):
    return {"url": STORE.resumable_upload_url(object_name, content_type)}


# ------------------------------------------------------------------
# S3 resumable-upload proxy.
#
# The dashboard uploader speaks the GCS resumable protocol (POST for a
# session -> Location header, then chunked PUTs with Content-Range answered
# by 308 + Range until the final chunk). On AWS these two endpoints speak
# that same protocol on top of an S3 multipart upload so the frontend needs
# no cloud-specific code. 8 MB frontend chunks satisfy S3's 5 MB minimum
# part size for all but the last part.
# ------------------------------------------------------------------


def _prune_expired_upload_sessions(now: datetime.datetime):
    expired = [
        token for token, session in S3_UPLOAD_SESSIONS.items() if session["expires_at"] <= now
    ]
    for token in expired:
        del S3_UPLOAD_SESSIONS[token]


@router.post("/storage/s3-upload-session")
def start_s3_upload_session(
    request: Request,
    object_name: str = Query(...),
    content_type: str = Query("application/octet-stream"),
):
    upload_id = STORE.start_multipart(object_name, content_type)
    token = secrets.token_urlsafe(24)
    now = datetime.datetime.now(datetime.timezone.utc)
    with S3_UPLOAD_SESSIONS_LOCK:
        _prune_expired_upload_sessions(now)
        S3_UPLOAD_SESSIONS[token] = {
            "object_name": object_name,
            "upload_id": upload_id,
            "parts": [],
            "bytes_received": 0,
            "expires_at": now + datetime.timedelta(seconds=S3_UPLOAD_SESSION_TTL_SECONDS),
        }
    session_url = f"{request.base_url}storage/s3-upload/{token}"
    return Response(status_code=201, headers={"Location": session_url})


@router.put("/storage/s3-upload/{token}")
async def s3_upload_chunk(token: str, request: Request):
    with S3_UPLOAD_SESSIONS_LOCK:
        session = S3_UPLOAD_SESSIONS.get(token)
    if session is None:
        raise HTTPException(status_code=404, detail="Upload session not found or expired")

    content_range = request.headers.get("Content-Range", "")
    # "bytes start-end/total"
    try:
        byte_range, total_str = content_range.replace("bytes ", "").split("/")
        start_str, end_str = byte_range.split("-")
        start, end, total = int(start_str), int(end_str), int(total_str)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Bad Content-Range: {content_range!r}")

    body = await request.body()
    part_number = len(session["parts"]) + 1
    etag = STORE.upload_part(session["object_name"], session["upload_id"], part_number, body)

    with S3_UPLOAD_SESSIONS_LOCK:
        session["parts"].append({"PartNumber": part_number, "ETag": etag})
        session["bytes_received"] = end + 1

    if end + 1 >= total:
        STORE.complete_multipart(session["object_name"], session["upload_id"], session["parts"])
        with S3_UPLOAD_SESSIONS_LOCK:
            S3_UPLOAD_SESSIONS.pop(token, None)
        return Response(status_code=200)

    headers = {"Range": f"bytes=0-{end}"}
    return Response(status_code=308, headers=headers)


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
    fallback_name = sanitized_object_name.split("/")[-1] or "download"
    safe_download_name = (download_name or fallback_name).replace('"', "").replace("'", "")
    disposition = f'attachment; filename="{safe_download_name}"'
    try:
        url = STORE.signed_download_url(sanitized_object_name, disposition)
    except BlobNotFound:
        raise HTTPException(status_code=404, detail=f"File '{sanitized_object_name}' not found")
    return {"url": url}


def prune_expired_batch_download_tokens(now_utc: datetime.datetime) -> None:
    expired_tokens = [
        token for token, entry in BATCH_DOWNLOAD_TOKENS.items() if entry["expires_at"] <= now_utc
    ]
    for token in expired_tokens:
        del BATCH_DOWNLOAD_TOKENS[token]


def parse_batch_download_payload(
    payload: Dict[str, Any],
) -> tuple[List[Dict[str, str]], List[Dict[str, str]], str]:
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
    return file_requests, folder_requests, archive_name


def build_batch_download_response(
    file_requests: List[Dict[str, str]],
    folder_requests: List[Dict[str, str]],
    archive_name: str,
) -> StreamingResponse:
    stream_done = object()
    stream_queue = queue.Queue(maxsize=32)
    stream_errors: List[BaseException] = []

    class ZipQueueWriter:
        def write(self, chunk: bytes) -> int:
            if chunk:
                stream_queue.put(chunk)
            return len(chunk)

        def flush(self) -> None:
            return None

        def seekable(self) -> bool:
            return False

    def write_archive_to_stream() -> None:
        try:
            with zipfile.ZipFile(
                ZipQueueWriter(), mode="w", compression=zipfile.ZIP_STORED
            ) as archive:
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

                def copy_blob_into_archive(blob_name: str, archive_path: str) -> None:
                    with archive.open(archive_path, "w") as target:
                        source = STORE.open_read(blob_name)
                        try:
                            while True:
                                chunk = source.read(1024 * 1024)
                                if not chunk:
                                    break
                                target.write(chunk)
                        finally:
                            source.close()

                for request in file_requests:
                    if STORE.get_blob_metadata(request["object_name"]) is None:
                        raise HTTPException(
                            status_code=404, detail=f"File '{request['object_name']}' not found"
                        )
                    archive_path = request["archive_path"]
                    if archive_path in written_entries:
                        continue
                    ensure_parent_directories("", archive_path)
                    copy_blob_into_archive(request["object_name"], archive_path)
                    written_entries.add(archive_path)
                for request in folder_requests:
                    prefix = request["prefix"]
                    archive_root = request["archive_root"]
                    ensure_directory_entry(archive_root)
                    added_content = False
                    for blob_meta in STORE.list_prefix(prefix):
                        if blob_meta["name"] == prefix:
                            continue
                        relative_path = blob_meta["name"][len(prefix) :]
                        if not relative_path:
                            continue
                        sanitized_relative = sanitize_archive_item_path(
                            relative_path, relative_path
                        )
                        archive_path = archive_path_for(archive_root, sanitized_relative)
                        if archive_path in written_entries:
                            continue
                        ensure_parent_directories(archive_root, sanitized_relative)
                        copy_blob_into_archive(blob_meta["name"], archive_path)
                        written_entries.add(archive_path)
                        added_content = True
                    if not added_content:
                        ensure_directory_entry(archive_root)
        except BaseException as error:
            stream_errors.append(error)
        finally:
            stream_queue.put(stream_done)

    def iterator():
        writer_thread = threading.Thread(target=write_archive_to_stream, daemon=True)
        writer_thread.start()
        while True:
            item = stream_queue.get()
            if item is stream_done:
                break
            yield item
        writer_thread.join()
        if stream_errors:
            error = stream_errors[0]
            if isinstance(error, HTTPException):
                raise error
            if isinstance(error, BlobNotFound):
                raise HTTPException(status_code=404, detail=str(error)) from error
            raise error

    headers = {"Content-Disposition": f'attachment; filename="{archive_name}"'}
    return StreamingResponse(iterator(), media_type="application/zip", headers=headers)


@router.post("/batch-download-ticket")
def create_batch_download_ticket(payload: Dict[str, Any]):
    file_requests, folder_requests, archive_name = parse_batch_download_payload(payload)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    token = secrets.token_urlsafe(24)
    with BATCH_DOWNLOAD_TOKENS_LOCK:
        prune_expired_batch_download_tokens(now_utc)
        BATCH_DOWNLOAD_TOKENS[token] = {
            "file_requests": file_requests,
            "folder_requests": folder_requests,
            "archive_name": archive_name,
            "expires_at": now_utc + datetime.timedelta(seconds=BATCH_DOWNLOAD_TOKEN_TTL_SECONDS),
        }
    return {"downloadUrl": f"/batch-download/{token}"}


@router.get("/batch-download/{token}")
def batch_download_by_ticket(token: str):
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    with BATCH_DOWNLOAD_TOKENS_LOCK:
        prune_expired_batch_download_tokens(now_utc)
        ticket = BATCH_DOWNLOAD_TOKENS.pop(token, None)
    if ticket is None:
        raise HTTPException(status_code=404, detail="Download token not found or expired")
    return build_batch_download_response(
        file_requests=ticket["file_requests"],
        folder_requests=ticket["folder_requests"],
        archive_name=ticket["archive_name"],
    )


@router.post("/batch-download")
def batch_download(payload: Dict[str, Any]):
    file_requests, folder_requests, archive_name = parse_batch_download_payload(payload)
    return build_batch_download_response(file_requests, folder_requests, archive_name)
