import io
import mimetypes
import shutil
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Request, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from google.cloud import storage
from starlette.concurrency import run_in_threadpool


router = APIRouter()


BUCKET_NAME = "burla-test-joe-shared-workspace"


def normalize_prefix(path: str) -> str:
    if not path or path == "/":
        return ""
    path = path.lstrip("/")
    if not path.endswith("/"):
        path += "/"
    return path


def file_entry_from_blob(blob: storage.Blob, cwd_prefix: str):
    name_only = blob.name[len(cwd_prefix) :]
    size = blob.size or 0
    ext = f".{name_only.split('.')[-1]}" if "." in name_only else ""
    updated = blob.updated or datetime.now(timezone.utc)
    iso = updated.isoformat()
    return {
        "name": name_only,
        "isFile": True,
        "size": size,
        "dateCreated": iso,
        "dateModified": iso,
        "hasChild": False,
        "type": ext,
    }


def folder_entry_from_prefix(prefix: str, cwd_prefix: str):
    name_only = prefix[len(cwd_prefix) :].rstrip("/")
    iso = datetime.now(timezone.utc).isoformat()
    return {
        "name": name_only,
        "isFile": False,
        "size": 0,
        "dateCreated": iso,
        "dateModified": iso,
        "hasChild": True,
        "type": "",
    }


@router.post("/api/sf/filemanager")
async def filemanager(request: Request):
    data = await request.json()
    action = (data.get("action") or data.get("Action") or "read").lower()
    path = data.get("path") or data.get("Path") or "/"
    cwd_prefix = normalize_prefix(path)

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    if action == "read":
        iterator = client.list_blobs(BUCKET_NAME, prefix=cwd_prefix, delimiter="/")
        files = []
        for blob in iterator:
            if blob.name == cwd_prefix:
                continue
            files.append(file_entry_from_blob(blob, cwd_prefix))

        folders = [folder_entry_from_prefix(p, cwd_prefix) for p in iterator.prefixes]

        cwd_name = path.rstrip("/").split("/")[-1] if path not in ("", "/") else "/"
        iso = datetime.now(timezone.utc).isoformat()
        cwd = {
            "name": cwd_name,
            "isFile": False,
            "size": 0,
            "dateCreated": iso,
            "dateModified": iso,
            "hasChild": True,
            "type": "",
            "filterPath": path,
        }
        return JSONResponse({"cwd": cwd, "files": folders + files})

    if action == "create":
        name = data.get("name") or data.get("Name")
        if not name:
            raise HTTPException(status_code=400, detail="Missing name")
        folder_marker = cwd_prefix + name.rstrip("/") + "/"
        blob = bucket.blob(folder_marker)
        blob.upload_from_string(b"")
        return JSONResponse({"success": True})

    if action == "delete":
        names: List[str] = data.get("names") or data.get("Names") or []
        for name in names:
            key = cwd_prefix + name
            if name.endswith("/"):
                # delete folder recursively
                for blob in client.list_blobs(BUCKET_NAME, prefix=key):
                    blob.delete()
            else:
                blob = bucket.blob(key)
                blob.delete()
        return JSONResponse({"success": True})

    if action == "rename":
        name = data.get("name") or data.get("Name")
        new_name = data.get("newName") or data.get("NewName")
        if not name or not new_name:
            raise HTTPException(status_code=400, detail="Missing name/newName")
        old_key = cwd_prefix + name
        if name.endswith("/"):
            old_prefix = old_key
            new_prefix = cwd_prefix + new_name.rstrip("/") + "/"
            for blob in client.list_blobs(BUCKET_NAME, prefix=old_prefix):
                dest_key = new_prefix + blob.name[len(old_prefix) :]
                bucket.copy_blob(blob, bucket, new_name=dest_key)
            for blob in client.list_blobs(BUCKET_NAME, prefix=old_prefix):
                blob.delete()
        else:
            src = bucket.blob(old_key)
            dest_key = cwd_prefix + new_name
            bucket.copy_blob(src, bucket, new_name=dest_key)
            src.delete()
        return JSONResponse({"success": True})

    return JSONResponse({"error": f"Unsupported action: {action}"}, status_code=400)


@router.post("/api/sf/upload")
async def upload(
    path: str = Form("/"),
    files: List[UploadFile] = File(default=[]),
):
    prefix = normalize_prefix(path)
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for upload_file in files:
        key = prefix + upload_file.filename
        blob = bucket.blob(key)
        blob.chunk_size = 8 * 1024 * 1024
        upload_file.file.seek(0)

        def _stream_copy() -> None:
            with blob.open("wb") as writer:
                shutil.copyfileobj(upload_file.file, writer, length=8 * 1024 * 1024)

        await run_in_threadpool(_stream_copy)

    return JSONResponse({"success": True})


@router.get("/api/sf/download")
async def download(path: str):
    if not path or path.endswith("/"):
        raise HTTPException(status_code=400, detail="Download requires a file path")
    key = path.lstrip("/")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(key)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="File not found")

    mime_type, _ = mimetypes.guess_type(key)
    mime_type = mime_type or "application/octet-stream"
    file_bytes = blob.download_as_bytes()
    headers = {"Content-Disposition": f"attachment; filename=\"{key.split('/')[-1]}\""}
    return StreamingResponse(io.BytesIO(file_bytes), media_type=mime_type, headers=headers)
