"""
Blob-store adapter for the /workspace/shared bucket: GCS on GCP, S3 on AWS,
Blob Storage on Azure (where the configured "bucket name" is the storage
account, with a fixed `shared-workspace` container inside it).

storage.py (the dashboard file-manager) is written against this interface so
its endpoint logic is identical on every cloud. Blob metadata is a plain dict:
{"name": str, "size": int, "updated": datetime | None}.

Uploads: the dashboard uploader speaks the GCS resumable protocol (session
POST returning a Location, then chunked PUTs with Content-Range answered by
308 + Range). GCS serves that natively via a signed URL. S3BlobStore and
AzureBlobStore return a head-local session URL instead; storage.py implements
the same protocol on top of an S3 multipart upload / Azure block list, so the
frontend is unchanged off GCP.
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


class AzureBlobStore:
    def __init__(self, storage_account: str):
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        from main_service.providers.azure import SHARED_WORKSPACE_CONTAINER

        self._account = storage_account
        self._service = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=DefaultAzureCredential(),
        )
        self._container = self._service.get_container_client(SHARED_WORKSPACE_CONTAINER)
        # Block-list uploads have no server-side upload id; commit needs the
        # content type from session start, so it is remembered per upload id.
        self._upload_content_types: dict[str, str] = {}

    def _meta(self, blob) -> dict:
        return {
            "name": blob.name,
            "size": blob.size or 0,
            "updated": blob.last_modified,
        }

    def get_blob_metadata(self, name: str) -> Optional[dict]:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            properties = self._container.get_blob_client(name).get_blob_properties()
        except ResourceNotFoundError:
            return None
        return self._meta(properties)

    def iter_pages(self, prefix: str, max_results: Optional[int] = None):
        from azure.storage.blob import BlobPrefix

        pages = self._container.walk_blobs(
            name_starts_with=prefix, delimiter="/", results_per_page=max_results
        ).by_page()
        for page in pages:
            prefixes, files = [], []
            for item in page:
                if isinstance(item, BlobPrefix):
                    prefixes.append(item.name)
                else:
                    files.append(self._meta(item))
            yield prefixes, files

    def list_prefix(self, prefix: str, max_results: Optional[int] = None) -> Iterator[dict]:
        import itertools

        blobs = self._container.list_blobs(name_starts_with=prefix)
        if max_results is not None:
            blobs = itertools.islice(blobs, max_results)
        for blob in blobs:
            yield self._meta(blob)

    def upload_empty(self, name: str, content_type: str):
        from azure.storage.blob import ContentSettings

        self._container.upload_blob(
            name,
            b"",
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )

    def delete_batch(self, names: list[str]):
        from azure.core.exceptions import ResourceNotFoundError

        for name in names:
            try:
                self._container.delete_blob(name)
            except ResourceNotFoundError:
                continue

    def rename(self, source: str, destination: str):
        from time import sleep

        from azure.core.exceptions import ResourceNotFoundError

        source_client = self._container.get_blob_client(source)
        try:
            # Same-account server-side copy; authorized by a short-lived
            # read SAS on the source since copy URLs carry their own auth.
            source_url = source_client.url + "?" + self._read_sas(source, minutes=15)
            destination_client = self._container.get_blob_client(destination)
            destination_client.start_copy_from_url(source_url)
            properties = destination_client.get_blob_properties()
            while properties.copy.status == "pending":
                sleep(0.5)
                properties = destination_client.get_blob_properties()
            if properties.copy.status != "success":
                raise Exception(f"Copy of '{source}' ended {properties.copy.status}")
        except ResourceNotFoundError:
            raise BlobNotFound(f"'{source}' not found")
        self._container.delete_blob(source)

    def open_read(self, name: str):
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return self._container.get_blob_client(name).download_blob()
        except ResourceNotFoundError:
            raise BlobNotFound(f"'{name}' not found")

    def _read_sas(self, name: str, minutes: int = 0, days: int = 0, disposition: str = None) -> str:
        from azure.storage.blob import BlobSasPermissions, generate_blob_sas

        from main_service.providers.azure import SHARED_WORKSPACE_CONTAINER

        now = datetime.datetime.now(datetime.timezone.utc)
        expiry = now + datetime.timedelta(minutes=minutes, days=days)
        delegation_key = self._service.get_user_delegation_key(now, expiry)
        return generate_blob_sas(
            account_name=self._account,
            container_name=SHARED_WORKSPACE_CONTAINER,
            blob_name=name,
            user_delegation_key=delegation_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry,
            content_disposition=disposition,
        )

    def signed_download_url(self, name: str, disposition: str) -> str:
        if self.get_blob_metadata(name) is None:
            raise BlobNotFound(f"'{name}' not found")
        # User-delegation SAS keys max out at 7 days, matching GCS/S3.
        sas = self._read_sas(name, days=7, disposition=disposition)
        return self._container.get_blob_client(name).url + "?" + sas

    def resumable_upload_url(self, name: str, content_type: str) -> str:
        # Served by storage.py's proxy endpoints, which mimic the GCS
        # resumable protocol on top of the block-list methods below.
        from urllib.parse import quote

        return f"/storage/s3-upload-session?object_name={quote(name)}&content_type={quote(content_type)}"

    # ---- "multipart" pieces used by the proxy endpoints (block blobs) ----

    def start_multipart(self, name: str, content_type: str) -> str:
        import uuid

        upload_id = uuid.uuid4().hex
        self._upload_content_types[upload_id] = content_type
        return upload_id

    def upload_part(self, name: str, upload_id: str, part_number: int, body: bytes) -> str:
        import base64

        block_id = base64.b64encode(f"{upload_id}-{part_number:06d}".encode()).decode()
        self._container.get_blob_client(name).stage_block(block_id, body)
        return block_id

    def complete_multipart(self, name: str, upload_id: str, parts: list[dict]):
        from azure.storage.blob import BlobBlock, ContentSettings

        ordered = sorted(parts, key=lambda part: part["PartNumber"])
        block_list = [BlobBlock(part["ETag"]) for part in ordered]
        content_type = self._upload_content_types.pop(upload_id, None)
        self._container.get_blob_client(name).commit_block_list(
            block_list,
            content_settings=ContentSettings(content_type=content_type),
        )


def get_blob_store(bucket_name: str):
    if CLOUD_PROVIDER == "aws":
        return S3BlobStore(bucket_name)
    if CLOUD_PROVIDER == "azure":
        return AzureBlobStore(bucket_name)
    return GCSBlobStore(bucket_name)
