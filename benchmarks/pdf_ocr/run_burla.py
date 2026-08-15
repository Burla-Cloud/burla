import argparse
import hashlib
import json
import os
import socket
import subprocess
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests
from burla import __version__ as burla_version
from burla import remote_parallel_map

OCR_IMAGE = (
    "public.ecr.aws/e2g5l2y4/burla-pdf-ocr-benchmark@"
    "sha256:924b5e226663f2f6f8df96f15dee75697eb6a8a7bae2314b5553b8039f09d7c8"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(url: str, destination: Path) -> None:
    with requests.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        with destination.open("wb") as output:
            for chunk in response.iter_content(1024 * 1024):
                output.write(chunk)


def put_file(url: str, path: Path) -> None:
    with path.open("rb") as file:
        response = requests.put(
            url,
            data=file,
            headers={"Content-Length": str(path.stat().st_size)},
            timeout=(30, 3600),
        )
    response.raise_for_status()


def put_json(url: str, value: dict) -> None:
    payload = json.dumps(value, indent=2).encode()
    response = requests.put(
        url,
        data=payload,
        headers={"Content-Length": str(len(payload))},
        timeout=(30, 300),
    )
    response.raise_for_status()


def ocr_document(task: dict) -> dict:
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        input_path = scratch_dir / "input.pdf"
        output_path = scratch_dir / "searchable.pdf"
        text_path = scratch_dir / "text.txt"

        download_started = time.perf_counter()
        download_file(task["input_url"], input_path)
        download_seconds = time.perf_counter() - download_started
        input_sha256 = sha256_file(input_path)
        if input_sha256 != task["input_sha256"]:
            raise RuntimeError(
                f"Input checksum mismatch for {task['document_id']}: "
                f"{input_sha256} != {task['input_sha256']}"
            )

        ocr_started = time.perf_counter()
        try:
            subprocess.run(
                [
                    "ocrmypdf",
                    "--force-ocr",
                    "--rotate-pages",
                    "--deskew",
                    "--output-type",
                    "pdf",
                    "--sidecar",
                    str(text_path),
                    "--quiet",
                    str(input_path),
                    str(output_path),
                ],
                check=True,
                text=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"OCRmyPDF failed for {task['document_id']} "
                f"(exit {error.returncode}):\n{error.stderr}"
            ) from error
        ocr_seconds = time.perf_counter() - ocr_started

        upload_started = time.perf_counter()
        put_file(task["output_pdf_url"], output_path)
        put_file(task["output_text_url"], text_path)
        upload_seconds = time.perf_counter() - upload_started
        result = {
            "document_id": task["document_id"],
            "pages": task["pages"],
            "source_bytes": input_path.stat().st_size,
            "output_pdf_bytes": output_path.stat().st_size,
            "output_text_bytes": text_path.stat().st_size,
            "input_sha256": input_sha256,
            "output_pdf_sha256": sha256_file(output_path),
            "output_text_sha256": sha256_file(text_path),
            "download_seconds": download_seconds,
            "ocr_seconds": ocr_seconds,
            "upload_seconds": upload_seconds,
            "total_seconds": time.perf_counter() - started,
            "worker_container": socket.gethostname(),
            "output_pdf_key": task["output_pdf_key"],
            "output_text_key": task["output_text_key"],
            "metadata_key": task["metadata_key"],
        }
        put_json(task["metadata_url"], result)
        return result


def presigned_task(s3, bucket: str, output_prefix: str, document: dict, expires: int):
    document_id = document["document_id"]
    output_pdf_key = f"{output_prefix}/documents/{document_id}/searchable.pdf"
    output_text_key = f"{output_prefix}/documents/{document_id}/text.txt"
    metadata_key = f"{output_prefix}/documents/{document_id}/metadata.json"
    return {
        "document_id": document_id,
        "pages": document["pages"],
        "input_sha256": document["sha256"],
        "input_url": s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": document["input_key"]},
            ExpiresIn=expires,
        ),
        "output_pdf_url": s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": output_pdf_key},
            ExpiresIn=expires,
        ),
        "output_text_url": s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": output_text_key},
            ExpiresIn=expires,
        ),
        "metadata_url": s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": metadata_key},
            ExpiresIn=expires,
        ),
        "output_pdf_key": output_pdf_key,
        "output_text_key": output_text_key,
        "metadata_key": metadata_key,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the GovDocs1 OCR benchmark on Burla."
    )
    parser.add_argument("--profile", default="burla-test")
    parser.add_argument("--bucket")
    parser.add_argument("--manifest-key", default="manifests/calibration-v1.json")
    parser.add_argument("--run-id")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--max-parallelism", type=int, default=8)
    parser.add_argument("--image", default=OCR_IMAGE)
    parser.add_argument("--url-expiration-seconds", type=int, default=43_200)
    parser.add_argument("--dashboard-url")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.dashboard_url:
        os.environ["BURLA_CLUSTER_DASHBOARD_URL"] = args.dashboard_url

    session = boto3.Session(profile_name=args.profile)
    account_id = session.client("sts").get_caller_identity()["Account"]
    bucket = args.bucket or f"burla-pdf-ocr-benchmark-{account_id}"
    s3 = session.client("s3")
    manifest = json.loads(
        s3.get_object(Bucket=bucket, Key=args.manifest_key)["Body"].read()
    )
    documents = manifest["documents"]
    if args.limit == 1:
        documents = [documents[len(documents) // 2]]
    elif args.limit:
        documents = [
            documents[round(i * (len(documents) - 1) / (args.limit - 1))]
            for i in range(args.limit)
        ]
    run_id = args.run_id or datetime.now(UTC).strftime("burla-%Y%m%dT%H%M%SZ")
    output_prefix = f"runs/{run_id}"
    tasks = [
        presigned_task(
            s3,
            bucket,
            output_prefix,
            document,
            args.url_expiration_seconds,
        )
        for document in documents
    ]

    started_at = datetime.now(UTC)
    started = time.perf_counter()
    results = remote_parallel_map(
        ocr_document,
        tasks,
        func_cpu="dynamic",
        func_ram="dynamic",
        image=args.image,
        grow=True,
        max_parallelism=args.max_parallelism,
        spinner=False,
    )
    wall_seconds = time.perf_counter() - started
    results.sort(key=lambda result: result["document_id"])
    total_pages = sum(result["pages"] for result in results)
    machines = []
    if args.dashboard_url:
        response = requests.get(
            f"{args.dashboard_url.rstrip('/')}/v1/cluster/nodes", timeout=30
        )
        response.raise_for_status()
        machines = [
            {
                "instance_name": node["instance_name"],
                "machine_type": node["machine_type"],
                "status": node["status"],
            }
            for node in response.json()["nodes"]
            if args.image in [container["image"] for container in node["containers"]]
        ]
    summary = {
        "schema_version": 1,
        "framework": "burla",
        "burla_version": burla_version,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "bucket": bucket,
        "manifest_key": args.manifest_key,
        "image": args.image,
        "max_parallelism": args.max_parallelism,
        "documents": len(results),
        "pages": total_pages,
        "wall_seconds": wall_seconds,
        "documents_per_second": len(results) / wall_seconds,
        "pages_per_second": total_pages / wall_seconds,
        "task_ocr_seconds": sum(result["ocr_seconds"] for result in results),
        "machines": machines,
        "documents_by_worker_container": dict(
            sorted(Counter(result["worker_container"] for result in results).items())
        ),
        "results": results,
    }
    summary_key = f"{output_prefix}/summary.json"
    s3.put_object(
        Bucket=bucket,
        Key=summary_key,
        Body=json.dumps(summary, indent=2).encode(),
        ContentType="application/json",
    )
    print(
        json.dumps(
            {
                "summary": f"s3://{bucket}/{summary_key}",
                "documents": len(results),
                "pages": total_pages,
                "wall_seconds": round(wall_seconds, 2),
                "pages_per_second": round(total_pages / wall_seconds, 2),
                "machines": len(machines),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
