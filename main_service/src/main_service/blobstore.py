"""
Blob-store adapter for the /workspace/shared bucket: GCS on GCP, S3 on AWS.

storage.py (the dashboard file-manager) is written against this interface so
its endpoint logic is identical on both clouds. Blob metadata is a plain dict:
{"name": str, "size": int, "updated": datetime | None}.

Uploads: the dashboard uploader speaks the GCS resumable protocol (session
POST returning a Location, then chunked PUTs with Content-Range answered by
308 + Range). GCS serves that natively via a signed URL. S3BlobStore returns
a head-local session URL instead; storage.py implements the same protocol on
top of an S3 multipart upload, so the frontend is unchanged on AWS.
"""

import datetime
from typing import Iterator, Optional

from main_service import PROJECT_ID, CLOUD_PROVIDER


class BlobNotFound(Exception):
    pass


class GCSBlobStore:
    def __init__(self, bucket_name: str):
        from google.cloud import storage
        from google.auth import default, impersonated_credentials

        # Impersonation makes it possible to create signed urls for any blobs
        # created with this client.
        source_creds, project_id = default()
        signing_creds = impersonated_credentials.Credentials(
            source_credentials=source_creds,
            target_principal=f"burla-main-service@{PROJECT_ID}.iam.gserviceaccount.com",
            target_scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
        )
        self._client = storage.Client(project=project_id)
        self._bucket = self._client.bucket(bucket_name)
        client_impersonated = storage.Client(project=project_id, credentials=signing_creds)
        self._bucket_impersonated = client_impersonated.bucket(bucket_name)

    def _meta(self, blob) -> dict:
        return {"name": blob.name, "size": blob.size or 0, "updated": blob.updated}

    def get_blob_metadata(self, name: str) -> Optional[dict]:
        blob = self._bucket.get_blob(name)
        return self._meta(blob) if blob else None

    def iter_pages(self, prefix: str, max_results: Optional[int] = None):
        """Yields (folder_prefixes, file_metas) per listing page."""
        iterator = self._bucket.list_blobs(
            prefix=prefix, delimiter="/", max_results=max_results
        )
        for page in iterator.pages:
            prefixes = list(getattr(page, "prefixes", []))
            files = [self._meta(blob) for blob in page]
            yield prefixes, files

    def list_prefix(self, prefix: str, max_results: Optional[int] = None) -> Iterator[dict]:
        for blob in self._bucket.list_blobs(prefix=prefix, max_results=max_results):
            yield self._meta(blob)

    def upload_empty(self, name: str, content_type: str):
        self._bucket.blob(name).upload_from_string(b"", content_type=content_type)

    def delete_batch(self, names: list[str]):
        from google.api_core.exceptions import NotFound

        with self._client.batch():
            for name in names:
                try:
                    self._bucket.blob(name).delete(client=self._client)
                except NotFound:
                    continue

    def rename(self, source: str, destination: str):
        blob = self._bucket.get_blob(source)
        if blob is None:
            raise BlobNotFound(f"'{source}' not found")
        self._bucket.rename_blob(blob, destination)

    def open_read(self, name: str):
        blob = self._bucket.get_blob(name)
        if blob is None:
            raise BlobNotFound(f"'{name}' not found")
        return blob.open("rb")

    def signed_download_url(self, name: str, disposition: str) -> str:
        blob = self._bucket_impersonated.blob(name)
        if not blob.exists():
            raise BlobNotFound(f"'{name}' not found")
        return blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(days=7),
            method="GET",
            response_disposition=disposition,
        )

    def resumable_upload_url(self, name: str, content_type: str) -> str:
        blob = self._bucket_impersonated.blob(name)
        return blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(days=7),
            method="POST",
            service_account_email=f"burla-main-service@{PROJECT_ID}.iam.gserviceaccount.com",
            content_type=content_type,
            headers={"x-goog-resumable": "start"},
        )


class S3BlobStore:
    def __init__(self, bucket_name: str):
        import boto3

        self._s3 = boto3.client("s3")
        self._bucket_name = bucket_name

    def _meta_from_object(self, obj: dict) -> dict:
        return {
            "name": obj["Key"],
            "size": obj.get("Size", obj.get("ContentLength", 0)) or 0,
            "updated": obj.get("LastModified"),
        }

    def get_blob_metadata(self, name: str) -> Optional[dict]:
        from botocore.exceptions import ClientError

        try:
            response = self._s3.head_object(Bucket=self._bucket_name, Key=name)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return {
            "name": name,
            "size": response.get("ContentLength", 0) or 0,
            "updated": response.get("LastModified"),
        }

    def iter_pages(self, prefix: str, max_results: Optional[int] = None):
        paginator = self._s3.get_paginator("list_objects_v2")
        pagination_config = {}
        if max_results is not None:
            pagination_config["MaxItems"] = max_results
        pages = paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=prefix,
            Delimiter="/",
            PaginationConfig=pagination_config,
        )
        for page in pages:
            prefixes = [p["Prefix"] for p in page.get("CommonPrefixes", [])]
            files = [self._meta_from_object(obj) for obj in page.get("Contents", [])]
            yield prefixes, files

    def list_prefix(self, prefix: str, max_results: Optional[int] = None) -> Iterator[dict]:
        paginator = self._s3.get_paginator("list_objects_v2")
        pagination_config = {}
        if max_results is not None:
            pagination_config["MaxItems"] = max_results
        pages = paginator.paginate(
            Bucket=self._bucket_name, Prefix=prefix, PaginationConfig=pagination_config
        )
        for page in pages:
            for obj in page.get("Contents", []):
                yield self._meta_from_object(obj)

    def upload_empty(self, name: str, content_type: str):
        self._s3.put_object(Bucket=self._bucket_name, Key=name, Body=b"", ContentType=content_type)

    def delete_batch(self, names: list[str]):
        objects = [{"Key": name} for name in names]
        self._s3.delete_objects(Bucket=self._bucket_name, Delete={"Objects": objects, "Quiet": True})

    def rename(self, source: str, destination: str):
        if self.get_blob_metadata(source) is None:
            raise BlobNotFound(f"'{source}' not found")
        self._s3.copy_object(
            Bucket=self._bucket_name,
            Key=destination,
            CopySource={"Bucket": self._bucket_name, "Key": source},
        )
        self._s3.delete_objects(
            Bucket=self._bucket_name, Delete={"Objects": [{"Key": source}], "Quiet": True}
        )

    def open_read(self, name: str):
        from botocore.exceptions import ClientError

        try:
            response = self._s3.get_object(Bucket=self._bucket_name, Key=name)
        except ClientError as error:
            if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                raise BlobNotFound(f"'{name}' not found")
            raise
        return response["Body"]

    def signed_download_url(self, name: str, disposition: str) -> str:
        if self.get_blob_metadata(name) is None:
            raise BlobNotFound(f"'{name}' not found")
        return self._s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self._bucket_name,
                "Key": name,
                "ResponseContentDisposition": disposition,
            },
            ExpiresIn=7 * 24 * 3600,
        )

    def resumable_upload_url(self, name: str, content_type: str) -> str:
        # Served by storage.py's proxy endpoints, which mimic the GCS
        # resumable protocol on top of an S3 multipart upload.
        from urllib.parse import quote

        return f"/storage/s3-upload-session?object_name={quote(name)}&content_type={quote(content_type)}"

    # ---- multipart pieces used by the proxy endpoints ----

    def start_multipart(self, name: str, content_type: str) -> str:
        response = self._s3.create_multipart_upload(
            Bucket=self._bucket_name, Key=name, ContentType=content_type
        )
        return response["UploadId"]

    def upload_part(self, name: str, upload_id: str, part_number: int, body: bytes) -> str:
        response = self._s3.upload_part(
            Bucket=self._bucket_name,
            Key=name,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        )
        return response["ETag"]

    def complete_multipart(self, name: str, upload_id: str, parts: list[dict]):
        self._s3.complete_multipart_upload(
            Bucket=self._bucket_name,
            Key=name,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )


def get_blob_store(bucket_name: str):
    if CLOUD_PROVIDER == "aws":
        return S3BlobStore(bucket_name)
    return GCSBlobStore(bucket_name)
