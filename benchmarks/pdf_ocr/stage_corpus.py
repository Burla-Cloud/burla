import argparse
import hashlib
import json
import os
import tempfile
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlencode

import boto3
import requests
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from burla import remote_parallel_map
from remotezip import RemoteIOError, RemoteZip

SOURCE_BUCKET = "digitalcorpora"
SOURCE_REGION = "us-west-2"
SOURCE_PREFIX = "corpora/files/govdocs1/zipfiles/"
SOURCE_HTTP_ROOT = "https://digitalcorpora.s3.us-west-2.amazonaws.com"
PUBLISHED_SHA1_URL = (
    "https://digitalcorpora.s3.amazonaws.com/"
    "corpora/files/govdocs1/zipfilelist-sha1.txt"
)
PUBLISHED_MD5_URL = (
    "https://digitalcorpora.s3.amazonaws.com/corpora/files/govdocs1/zipfilelist-md5.txt"
)


def source_s3_client():
    return boto3.client(
        "s3",
        region_name=SOURCE_REGION,
        config=Config(signature_version=UNSIGNED),
    )


def checksum_manifest(url: str) -> dict[str, str]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return {
        Path(line.split(maxsplit=1)[1]).name: line.split(maxsplit=1)[0]
        for line in response.text.splitlines()
        if line.strip()
    }


def source_archives() -> list[dict]:
    sha1_by_name = checksum_manifest(PUBLISHED_SHA1_URL)
    md5_by_name = checksum_manifest(PUBLISHED_MD5_URL)
    versions = []
    paginator = source_s3_client().get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=SOURCE_PREFIX):
        for version in page.get("Versions", []):
            name = Path(version["Key"]).name
            stem = Path(name).stem
            if (
                version["IsLatest"]
                and stem.isdigit()
                and len(stem) == 3
                and name.endswith(".zip")
            ):
                query = urlencode({"versionId": version["VersionId"]})
                versions.append(
                    {
                        "archive_name": name,
                        "source_key": version["Key"],
                        "source_version_id": version["VersionId"],
                        "source_size": version["Size"],
                        "source_etag": version["ETag"].strip('"'),
                        "source_last_modified": version["LastModified"].isoformat(),
                        "source_url": (
                            f"{SOURCE_HTTP_ROOT}/{quote(version['Key'], safe='/')}?{query}"
                        ),
                        "published_sha1": sha1_by_name.get(name),
                        "published_md5": md5_by_name.get(name),
                    }
                )
    return sorted(versions, key=lambda archive: archive["archive_name"])


def ensure_bucket(s3, bucket: str, region: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response["Error"]["Code"] != "404":
            raise
        create_args = {"Bucket": bucket}
        if region != "us-east-1":
            create_args["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**create_args)

    s3.put_public_access_block(
        Bucket=bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    s3.put_bucket_ownership_controls(
        Bucket=bucket,
        OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
    )
    s3.put_bucket_encryption(
        Bucket=bucket,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
            ]
        },
    )
    s3.put_bucket_tagging(
        Bucket=bucket,
        Tagging={"TagSet": [{"Key": "project", "Value": "burla-govdocs1-corpus"}]},
    )


def put_file(url: str, path: Path) -> None:
    with path.open("rb") as file:
        response = requests.put(
            url,
            data=file,
            headers={"Content-Length": str(path.stat().st_size)},
            timeout=(30, 3600),
        )
    if response.status_code >= 400:
        raise RuntimeError(f"S3 upload failed with status {response.status_code}")


def put_bytes(url: str, payload: bytes) -> None:
    response = requests.put(
        url,
        data=payload,
        headers={"Content-Length": str(len(payload))},
        timeout=(30, 300),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"S3 upload failed with status {response.status_code}")


def copy_archive(task: dict) -> dict:
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as scratch:
        archive_path = Path(scratch) / task["archive_name"]
        for attempt in range(3):
            md5 = hashlib.md5(usedforsecurity=False)
            sha1 = hashlib.sha1(usedforsecurity=False)
            sha256 = hashlib.sha256()
            try:
                with requests.get(
                    task["source_url"], stream=True, timeout=(30, 120)
                ) as response:
                    if response.status_code >= 400:
                        raise RuntimeError(
                            f"Source download failed with status {response.status_code}"
                        )
                    with archive_path.open("wb") as output:
                        for chunk in response.iter_content(1024 * 1024):
                            output.write(chunk)
                            md5.update(chunk)
                            sha1.update(chunk)
                            sha256.update(chunk)
                actual_size = archive_path.stat().st_size
                if actual_size != task["source_size"]:
                    raise RuntimeError(
                        f"{task['archive_name']} size mismatch: "
                        f"{actual_size} != {task['source_size']}"
                    )
                put_file(task["destination_put_url"], archive_path)
                break
            except (requests.RequestException, RuntimeError):
                if attempt == 2:
                    raise
                time.sleep(2**attempt)

    actual_md5 = md5.hexdigest()
    actual_sha1 = sha1.hexdigest()
    result = {
        **{
            key: task[key]
            for key in (
                "archive_name",
                "source_key",
                "source_version_id",
                "source_size",
                "source_etag",
                "source_last_modified",
                "published_sha1",
                "published_md5",
                "destination_key",
            )
        },
        "actual_md5": actual_md5,
        "actual_sha1": actual_sha1,
        "actual_sha256": sha256.hexdigest(),
        "published_md5_matches": task["published_md5"] == actual_md5,
        "published_sha1_matches": task["published_sha1"] == actual_sha1,
        "elapsed_seconds": time.perf_counter() - started,
    }
    put_bytes(
        task["result_put_url"],
        json.dumps(result, separators=(",", ":")).encode(),
    )
    return result


def index_archive(task: dict) -> dict:
    try:
        with RemoteZip(task["archive_get_url"], initial_buffer_size=1024 * 1024) as zf:
            members = []
            for info in zf.infolist():
                if info.is_dir() or not info.filename.lower().endswith(".pdf"):
                    continue
                document_id = hashlib.sha256(
                    f"{task['archive_name']}:{info.filename}".encode()
                ).hexdigest()[:24]
                members.append(
                    {
                        "document_id": document_id,
                        "archive_name": task["archive_name"],
                        "archive_key": task["archive_key"],
                        "member_name": info.filename,
                        "header_offset": info.header_offset,
                        "compressed_size": info.compress_size,
                        "uncompressed_size": info.file_size,
                        "compression_method": info.compress_type,
                        "crc32": info.CRC,
                        "flag_bits": info.flag_bits,
                        "text_key": (f"text/{document_id[:2]}/{document_id}.txt"),
                    }
                )
    except (requests.RequestException, RemoteIOError, zipfile.BadZipFile) as error:
        raise RuntimeError(
            f"Failed to index {task['archive_name']}: {type(error).__name__}"
        ) from None

    payload = b"".join(
        json.dumps(member, separators=(",", ":")).encode() + b"\n" for member in members
    )
    put_bytes(task["index_put_url"], payload)
    return {
        "archive_name": task["archive_name"],
        "archive_key": task["archive_key"],
        "index_key": task["index_key"],
        "pdf_count": len(members),
        "members": members,
    }


def write_json(s3, bucket: str, key: str, value) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(value, indent=2).encode(),
        ContentType="application/json",
    )


def selected_archives(args) -> list[dict]:
    archives = source_archives()
    if args.archives:
        names = {
            name if name.endswith(".zip") else f"{name}.zip" for name in args.archives
        }
        archives = [archive for archive in archives if archive["archive_name"] in names]
    if args.limit_archives:
        archives = archives[: args.limit_archives]
    return archives


def copy_sidecars(args, s3, bucket: str) -> dict[str, dict]:
    prefix = f"manifests/{args.run_id}/archive-copy/"
    results = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            result = json.loads(
                s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
            )
            results[result["archive_name"]] = result
    return results


def run_copy(args, s3, bucket: str, archives: list[dict]) -> list[dict]:
    completed = copy_sidecars(args, s3, bucket)
    tasks = []
    for archive in archives:
        if archive["archive_name"] in completed:
            continue
        destination_key = f"raw/zipfiles/{archive['archive_name']}"
        result_key = (
            f"manifests/{args.run_id}/archive-copy/"
            f"{Path(archive['archive_name']).stem}.json"
        )
        tasks.append(
            {
                **archive,
                "destination_key": destination_key,
                "destination_put_url": s3.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": bucket, "Key": destination_key},
                    ExpiresIn=args.url_expiration_seconds,
                ),
                "result_put_url": s3.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": bucket, "Key": result_key},
                    ExpiresIn=args.url_expiration_seconds,
                ),
            }
        )
    new_results = []
    if tasks:
        new_results = remote_parallel_map(
            copy_archive,
            tasks,
            func_cpu=16,
            func_ram="dynamic",
            grow=True,
            max_parallelism=min(args.copy_parallelism, len(tasks)),
            spinner=False,
        )
    results = [
        completed[archive["archive_name"]]
        for archive in archives
        if archive["archive_name"] in completed
    ] + new_results
    results.sort(key=lambda result: result["archive_name"])
    manifest = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "source_bucket": SOURCE_BUCKET,
        "source_prefix": SOURCE_PREFIX,
        "archive_count": len(results),
        "total_bytes": sum(result["source_size"] for result in results),
        "published_md5_mismatches": [
            result["archive_name"]
            for result in results
            if not result["published_md5_matches"]
        ],
        "published_sha1_mismatches": [
            result["archive_name"]
            for result in results
            if not result["published_sha1_matches"]
        ],
        "archives": results,
    }
    write_json(
        s3,
        bucket,
        f"manifests/{args.run_id}/raw-archives.json",
        manifest,
    )
    return results


def load_copy_results(args, s3, bucket: str) -> list[dict]:
    key = f"manifests/{args.run_id}/raw-archives.json"
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())["archives"]


def run_index(args, s3, bucket: str, archives: list[dict]) -> list[dict]:
    tasks = []
    for archive in archives:
        index_key = (
            f"manifests/{args.run_id}/archive-index/"
            f"{Path(archive['archive_name']).stem}.jsonl"
        )
        tasks.append(
            {
                "archive_name": archive["archive_name"],
                "archive_key": archive["destination_key"],
                "archive_get_url": s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket, "Key": archive["destination_key"]},
                    ExpiresIn=args.url_expiration_seconds,
                ),
                "index_key": index_key,
                "index_put_url": s3.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": bucket, "Key": index_key},
                    ExpiresIn=args.url_expiration_seconds,
                ),
            }
        )
    results = remote_parallel_map(
        index_archive,
        tasks,
        func_cpu=1,
        func_ram=4,
        grow=True,
        max_parallelism=min(args.index_parallelism, len(tasks)),
        spinner=False,
    )
    results.sort(key=lambda result: result["archive_name"])
    members = [
        member for archive_result in results for member in archive_result.pop("members")
    ]
    members.sort(key=lambda member: (member["archive_name"], member["member_name"]))
    member_payload = b"".join(
        json.dumps(member, separators=(",", ":")).encode() + b"\n" for member in members
    )
    member_key = f"manifests/{args.run_id}/pdf-members.jsonl"
    s3.put_object(Bucket=bucket, Key=member_key, Body=member_payload)
    summary = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "archive_count": len(results),
        "pdf_count": len(members),
        "total_compressed_bytes": sum(member["compressed_size"] for member in members),
        "total_uncompressed_bytes": sum(
            member["uncompressed_size"] for member in members
        ),
        "member_manifest_key": member_key,
        "archives": results,
    }
    write_json(
        s3,
        bucket,
        f"manifests/{args.run_id}/pdf-members-summary.json",
        summary,
    )
    return members


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy and index the complete official GovDocs1 corpus with Burla."
    )
    parser.add_argument("--profile", default="burla-test")
    parser.add_argument("--bucket")
    parser.add_argument("--dashboard-url")
    parser.add_argument("--phase", choices=("copy", "index", "all"), default="all")
    parser.add_argument("--run-id", default="govdocs1-v1")
    parser.add_argument("--archives", nargs="*")
    parser.add_argument("--limit-archives", type=int)
    parser.add_argument("--copy-parallelism", type=int, default=64)
    parser.add_argument("--index-parallelism", type=int, default=128)
    parser.add_argument("--url-expiration-seconds", type=int, default=43_200)
    parser.add_argument("--enable-versioning", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dashboard_url:
        os.environ["BURLA_CLUSTER_DASHBOARD_URL"] = args.dashboard_url
    session = boto3.Session(profile_name=args.profile)
    account_id = session.client("sts").get_caller_identity()["Account"]
    region = session.region_name or "us-east-1"
    bucket = args.bucket or f"burla-govdocs1-corpus-{account_id}"
    s3 = session.client("s3")
    ensure_bucket(s3, bucket, region)

    archives = selected_archives(args)
    if args.phase in {"copy", "all"}:
        copy_results = run_copy(args, s3, bucket, archives)
    else:
        copy_results = load_copy_results(args, s3, bucket)
        if args.archives:
            names = {
                name if name.endswith(".zip") else f"{name}.zip"
                for name in args.archives
            }
            copy_results = [
                archive for archive in copy_results if archive["archive_name"] in names
            ]
        if args.limit_archives:
            copy_results = copy_results[: args.limit_archives]

    members = []
    if args.phase in {"index", "all"}:
        members = run_index(args, s3, bucket, copy_results)
    if args.enable_versioning:
        s3.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )
    print(
        json.dumps(
            {
                "bucket": bucket,
                "phase": args.phase,
                "archives": len(copy_results),
                "pdf_members": len(members),
                "run_id": args.run_id,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
