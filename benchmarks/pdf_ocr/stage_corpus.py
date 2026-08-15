import argparse
import hashlib
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError
from pypdf import PdfReader

DATASET_REPOSITORY = "BEE-spoke-data/govdocs1-pdf-source"
SAMPLE_TREE_URL = (
    f"https://huggingface.co/api/datasets/{DATASET_REPOSITORY}/tree/main/sample"
)
SAMPLE_DOWNLOAD_URL = (
    f"https://huggingface.co/datasets/{DATASET_REPOSITORY}/resolve/main"
)


def list_sample_pdfs() -> list[dict]:
    url = SAMPLE_TREE_URL
    params = {"recursive": "true", "expand": "false", "limit": 100}
    files = []
    while url:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        files.extend(
            item
            for item in response.json()
            if item["type"] == "file" and item["path"].endswith(".pdf")
        )
        url = response.links.get("next", {}).get("url")
        params = None
    return files


def select_size_quantiles(files: list[dict], count: int) -> list[dict]:
    files = sorted(files, key=lambda item: (item["size"], item["path"]))
    if count == 1:
        return [files[len(files) // 2]]
    return [files[round(i * (len(files) - 1) / (count - 1))] for i in range(count)]


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
        Tagging={"TagSet": [{"Key": "project", "Value": "burla-pdf-ocr-benchmark"}]},
    )


def download_document(source_path: str, destination: Path) -> tuple[str, int]:
    url = f"{SAMPLE_DOWNLOAD_URL}/{source_path}?download=true"
    digest = hashlib.sha256()
    size = 0
    with requests.get(url, stream=True, timeout=(30, 300)) as response:
        response.raise_for_status()
        with destination.open("wb") as output:
            for chunk in response.iter_content(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
                size += len(chunk)
    return digest.hexdigest(), size


def stage_document(s3, bucket: str, source: dict, scratch_dir: Path) -> dict:
    document_id = Path(source["path"]).stem
    local_path = scratch_dir / f"{document_id}.pdf"
    sha256, size = download_document(source["path"], local_path)
    pages = len(PdfReader(local_path, strict=False).pages)
    key = f"datasets/govdocs1/inputs/{document_id}.pdf"
    source_url = f"{SAMPLE_DOWNLOAD_URL}/{source['path']}?download=true"
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/pdf",
            "Metadata": {"sha256": sha256, "source": source_url},
        },
    )
    return {
        "document_id": document_id,
        "input_key": key,
        "pages": pages,
        "bytes": size,
        "sha256": sha256,
        "source_url": source_url,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stage a deterministic GovDocs1 calibration corpus in S3."
    )
    parser.add_argument("--profile", default="burla-test")
    parser.add_argument("--bucket")
    parser.add_argument("--count", type=int, default=32)
    parser.add_argument("--manifest-name", default="calibration-v1")
    parser.add_argument("--min-bytes", type=int, default=10_000)
    parser.add_argument("--max-bytes", type=int, default=50_000_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = boto3.Session(profile_name=args.profile)
    account_id = session.client("sts").get_caller_identity()["Account"]
    region = session.region_name or "us-east-1"
    bucket = args.bucket or f"burla-pdf-ocr-benchmark-{account_id}"
    s3 = session.client("s3")
    ensure_bucket(s3, bucket, region)

    files = [
        item
        for item in list_sample_pdfs()
        if args.min_bytes <= item["size"] <= args.max_bytes
    ]
    selected = select_size_quantiles(files, args.count)
    with tempfile.TemporaryDirectory() as scratch:
        documents = [
            stage_document(s3, bucket, source, Path(scratch)) for source in selected
        ]

    manifest = {
        "schema_version": 1,
        "name": args.manifest_name,
        "created_at": datetime.now(UTC).isoformat(),
        "source": {
            "dataset": DATASET_REPOSITORY,
            "subset": "sample",
            "license": "GovDocs1 files are freely redistributable",
        },
        "selection": {
            "method": "size_quantiles",
            "available_documents": len(files),
            "count": len(documents),
            "min_bytes": args.min_bytes,
            "max_bytes": args.max_bytes,
        },
        "total_pages": sum(document["pages"] for document in documents),
        "total_bytes": sum(document["bytes"] for document in documents),
        "documents": documents,
    }
    manifest_key = f"manifests/{args.manifest_name}.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    print(
        json.dumps(
            {
                "manifest": f"s3://{bucket}/{manifest_key}",
                "documents": len(documents),
                "pages": manifest["total_pages"],
                "bytes": manifest["total_bytes"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
