import argparse
import binascii
import bz2
import hashlib
import json
import logging
import os
import socket
import struct
import subprocess
import tempfile
import time
import zlib
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError
from burla import __version__ as burla_version
from burla import remote_parallel_map
from pypdf import PdfReader
from pypdf.errors import PyPdfError

OCR_IMAGE = (
    "public.ecr.aws/e2g5l2y4/burla-pdf-ocr-benchmark@"
    "sha256:ccd4d8d959e922da7017cfd12e93a0d6ec47a738aea45301aaeeea691ab8d74a"
)
LOCAL_FILE_HEADER_FORMAT = "<IHHHHHIIIHH"
LOCAL_FILE_HEADER_SIZE = 30
LOCAL_FILE_SIGNATURE = 0x04034B50
logging.getLogger("pypdf").setLevel(logging.ERROR)


class DocumentError(RuntimeError):
    pass


def range_response(url: str, start: int, end: int):
    for attempt in range(5):
        try:
            response = requests.get(
                url,
                headers={"Range": f"bytes={start}-{end}"},
                stream=True,
                timeout=(30, 1800),
            )
            if response.status_code == 206:
                return response
            response.close()
            if attempt == 4:
                raise RuntimeError(
                    f"S3 range read failed with status {response.status_code}"
                )
        except requests.RequestException:
            if attempt == 4:
                raise
        time.sleep(2**attempt)


def extract_member(task: dict, archive_url: str, destination: Path) -> tuple[str, int]:
    if task["flag_bits"] & 1:
        raise DocumentError("ZIP member is encrypted")

    header_response = range_response(
        archive_url,
        task["header_offset"],
        task["header_offset"] + LOCAL_FILE_HEADER_SIZE - 1,
    )
    header = header_response.content
    header_response.close()
    (
        signature,
        _version,
        _flags,
        compression_method,
        _modified_time,
        _modified_date,
        _crc32,
        _compressed_size,
        _uncompressed_size,
        filename_length,
        extra_length,
    ) = struct.unpack(LOCAL_FILE_HEADER_FORMAT, header)
    if signature != LOCAL_FILE_SIGNATURE:
        raise DocumentError("Invalid ZIP local-file header")
    if compression_method != task["compression_method"]:
        raise DocumentError("ZIP compression method differs from central directory")

    data_start = (
        task["header_offset"] + LOCAL_FILE_HEADER_SIZE + filename_length + extra_length
    )
    data_end = data_start + task["compressed_size"] - 1
    if compression_method == 0:
        decompressor = None
    elif compression_method == 8:
        decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
    elif compression_method == 12:
        decompressor = bz2.BZ2Decompressor()
    else:
        raise DocumentError(f"Unsupported ZIP compression method {compression_method}")

    crc32 = 0
    digest = hashlib.sha256()
    uncompressed_size = 0
    with (
        range_response(archive_url, data_start, data_end) as response,
        destination.open("wb") as output,
    ):
        for compressed_chunk in response.iter_content(1024 * 1024):
            chunk = (
                compressed_chunk
                if decompressor is None
                else decompressor.decompress(compressed_chunk)
            )
            output.write(chunk)
            crc32 = binascii.crc32(chunk, crc32)
            digest.update(chunk)
            uncompressed_size += len(chunk)
        if compression_method == 8:
            chunk = decompressor.flush()
            output.write(chunk)
            crc32 = binascii.crc32(chunk, crc32)
            digest.update(chunk)
            uncompressed_size += len(chunk)

    if uncompressed_size != task["uncompressed_size"]:
        raise DocumentError(
            f"Uncompressed size mismatch: "
            f"{uncompressed_size} != {task['uncompressed_size']}"
        )
    if crc32 & 0xFFFFFFFF != task["crc32"]:
        raise DocumentError("ZIP CRC32 mismatch")
    with destination.open("rb") as file:
        if b"%PDF-" not in file.read(1024):
            raise DocumentError("Extracted member has no PDF signature")
    return digest.hexdigest(), uncompressed_size


def upload_text(post: dict, key: str, payload: bytes) -> None:
    for attempt in range(5):
        fields = dict(post["fields"])
        fields["key"] = key
        try:
            response = requests.post(
                post["url"],
                data=fields,
                files={"file": (Path(key).name, payload)},
                timeout=(30, 1800),
            )
            if response.status_code in {200, 201, 204}:
                return
            if attempt == 4:
                raise RuntimeError(
                    f"S3 text upload failed with status {response.status_code}"
                )
        except requests.RequestException:
            if attempt == 4:
                raise
        time.sleep(2**attempt)


def text_pages(reader: PdfReader) -> int:
    count = 0
    for page in reader.pages:
        try:
            count += bool((page.extract_text() or "").strip())
        except (PyPdfError, IndexError, TypeError, ValueError):
            pass
    return count


def process_document(
    task: dict, archive_urls: dict[str, str], output_post: dict, output_prefix: str
) -> dict:
    logging.getLogger("pypdf").setLevel(logging.ERROR)
    started = time.perf_counter()
    base_result = {
        "document_id": task["document_id"],
        "archive_name": task["archive_name"],
        "member_name": task["member_name"],
        "compression_method": task["compression_method"],
        "compressed_size": task["compressed_size"],
        "uncompressed_size": task["uncompressed_size"],
        "worker_container": socket.gethostname(),
    }
    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)
        input_path = scratch_dir / "input.pdf"
        searchable_path = scratch_dir / "searchable.pdf"
        text_path = scratch_dir / "text.txt"

        extract_started = time.perf_counter()
        try:
            input_sha256, source_bytes = extract_member(
                task, archive_urls[task["archive_name"]], input_path
            )
        except DocumentError as error:
            return {
                **base_result,
                "status": "failed",
                "error_type": type(error).__name__,
                "error": str(error),
                "total_seconds": time.perf_counter() - started,
            }
        extract_seconds = time.perf_counter() - extract_started

        try:
            parse_started = time.perf_counter()
            reader = PdfReader(input_path, strict=False)
            if reader.is_encrypted:
                raise DocumentError("PDF is encrypted")
            page_count = len(reader.pages)
            direct_text_pages = text_pages(reader)
            ocr_pages = page_count - direct_text_pages

            ocr_seconds = 0.0
            text_source = input_path
            if ocr_pages:
                ocr_started = time.perf_counter()
                subprocess.run(
                    [
                        "ocrmypdf",
                        "--skip-text",
                        "--rotate-pages",
                        "--deskew",
                        "--output-type",
                        "pdf",
                        "--quiet",
                        str(input_path),
                        str(searchable_path),
                    ],
                    check=True,
                    text=True,
                    capture_output=True,
                )
                ocr_seconds = time.perf_counter() - ocr_started
                text_source = searchable_path

            subprocess.run(
                ["pdftotext", str(text_source), str(text_path)],
                check=True,
                text=True,
                capture_output=True,
            )
            text_payload = text_path.read_bytes()
            text_sha256 = hashlib.sha256(text_payload).hexdigest()
        # Malformed PDFs make third-party parsers raise assorted built-ins;
        # this boundary keeps one document from aborting the corpus.
        except Exception as error:  # noqa: BLE001
            return {
                **base_result,
                "status": "failed",
                "source_bytes": source_bytes,
                "source_sha256": input_sha256,
                "error_type": type(error).__name__,
                "error": str(error),
                "total_seconds": time.perf_counter() - started,
            }

        text_key = f"{output_prefix}/{task['text_key']}"
        upload_started = time.perf_counter()
        upload_text(output_post, text_key, text_payload)
        upload_seconds = time.perf_counter() - upload_started
        return {
            **base_result,
            "status": "succeeded",
            "source_bytes": source_bytes,
            "source_sha256": input_sha256,
            "page_count": page_count,
            "direct_text_pages": direct_text_pages,
            "ocr_pages": ocr_pages,
            "text_bytes": len(text_payload),
            "text_sha256": text_sha256,
            "text_key": text_key,
            "extract_seconds": extract_seconds,
            "parse_seconds": time.perf_counter() - parse_started,
            "ocr_seconds": ocr_seconds,
            "upload_seconds": upload_seconds,
            "total_seconds": time.perf_counter() - started,
        }


def load_jsonl(s3, bucket: str, key: str) -> list[dict]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    return [json.loads(line) for line in body.iter_lines() if line]


def jsonl_payload(values: list[dict]) -> bytes:
    return b"".join(
        json.dumps(value, separators=(",", ":")).encode() + b"\n" for value in values
    )


def select_documents(documents: list[dict], args) -> list[dict]:
    if args.archives:
        names = {
            name if name.endswith(".zip") else f"{name}.zip" for name in args.archives
        }
        documents = [
            document for document in documents if document["archive_name"] in names
        ]
    if args.limit == 1:
        return [documents[len(documents) // 2]]
    if args.limit:
        return [
            documents[round(i * (len(documents) - 1) / (args.limit - 1))]
            for i in range(args.limit)
        ]
    return documents


def parse_args():
    parser = argparse.ArgumentParser(
        description="Range-extract and convert GovDocs1 PDFs to plain text on Burla."
    )
    parser.add_argument("--profile", default="burla-test")
    parser.add_argument("--bucket")
    parser.add_argument("--corpus-run-id", default="govdocs1-v1")
    parser.add_argument("--run-id")
    parser.add_argument("--archives", nargs="*")
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
    bucket = args.bucket or f"burla-govdocs1-corpus-{account_id}"
    s3 = session.client("s3")
    member_key = f"manifests/{args.corpus_run_id}/pdf-members.jsonl"
    documents = select_documents(load_jsonl(s3, bucket, member_key), args)
    run_id = args.run_id or datetime.now(UTC).strftime("burla-text-%Y%m%dT%H%M%SZ")
    output_prefix = f"runs/{run_id}"
    partial_key = f"{output_prefix}/results.partial.jsonl"
    try:
        prior_results = load_jsonl(s3, bucket, partial_key)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchKey":
            raise
        prior_results = []
    completed_ids = {result["document_id"] for result in prior_results}
    pending_documents = [
        document
        for document in documents
        if document["document_id"] not in completed_ids
    ]
    archive_names = sorted({document["archive_name"] for document in pending_documents})
    archive_urls = {
        archive_name: s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": f"raw/zipfiles/{archive_name}"},
            ExpiresIn=args.url_expiration_seconds,
        )
        for archive_name in archive_names
    }
    output_post = s3.generate_presigned_post(
        Bucket=bucket,
        Key=f"{output_prefix}/text/${{filename}}",
        Fields={"x-amz-server-side-encryption": "AES256"},
        Conditions=[
            ["starts-with", "$key", f"{output_prefix}/text/"],
            {"x-amz-server-side-encryption": "AES256"},
        ],
        ExpiresIn=args.url_expiration_seconds,
    )

    def extract_text(task):
        return process_document(task, archive_urls, output_post, output_prefix)

    extract_text.__name__ = "extract_text"
    started_at = datetime.now(UTC)
    started = time.perf_counter()
    new_results = []
    if pending_documents:
        result_generator = remote_parallel_map(
            extract_text,
            pending_documents,
            func_cpu="dynamic",
            func_ram="dynamic",
            image=args.image,
            grow=True,
            max_parallelism=args.max_parallelism,
            generator=True,
            spinner=False,
        )
        try:
            new_results.extend(result_generator)
        except Exception:
            s3.put_object(
                Bucket=bucket,
                Key=partial_key,
                Body=jsonl_payload(prior_results + new_results),
            )
            raise
    wall_seconds = time.perf_counter() - started
    results = prior_results + new_results
    results.sort(key=lambda result: result["document_id"])
    result_payload = jsonl_payload(results)
    result_key = f"{output_prefix}/results.jsonl"
    s3.put_object(Bucket=bucket, Key=result_key, Body=result_payload)
    if prior_results:
        s3.delete_object(Bucket=bucket, Key=partial_key)
    succeeded = [result for result in results if result["status"] == "succeeded"]
    failed = [result for result in results if result["status"] == "failed"]
    summary = {
        "schema_version": 1,
        "framework": "burla",
        "burla_version": burla_version,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "corpus_run_id": args.corpus_run_id,
        "image": args.image,
        "max_parallelism": args.max_parallelism,
        "documents": len(results),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "pages": sum(result["page_count"] for result in succeeded),
        "direct_text_pages": sum(result["direct_text_pages"] for result in succeeded),
        "ocr_pages": sum(result["ocr_pages"] for result in succeeded),
        "text_bytes": sum(result["text_bytes"] for result in succeeded),
        "wall_seconds": wall_seconds,
        "documents_per_second": len(results) / wall_seconds,
        "documents_by_worker_container": dict(
            sorted(Counter(result["worker_container"] for result in results).items())
        ),
        "failure_types": dict(
            sorted(Counter(result["error_type"] for result in failed).items())
        ),
        "results_key": result_key,
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
                "succeeded": len(succeeded),
                "failed": len(failed),
                "pages": summary["pages"],
                "wall_seconds": round(wall_seconds, 2),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
