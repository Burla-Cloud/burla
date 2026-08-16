# GovDocs1 PDF text extraction benchmark

This benchmark processes every PDF in the official GovDocs1 ZIP corpus. It
extracts embedded text directly and uses OCRmyPDF only when a PDF has pages
without text. The only per-document output is plain text.

The private bucket is
`s3://burla-govdocs1-corpus-002645521087`. Workers receive presigned URLs and no
AWS credentials. The immutable worker image is:

```text
public.ecr.aws/e2g5l2y4/burla-pdf-ocr-benchmark@sha256:ccd4d8d959e922da7017cfd12e93a0d6ec47a738aea45301aaeeea691ab8d74a
```

## Run

Start remote development from the repository root:

```bash
AWS_PROFILE=burla-test DISABLE_BURLA_TELEMETRY=True make remote-dev
make cluster-info
```

Stage and index all 1,000 official archives:

```bash
BURLA_ENVIRONMENT=test uv run python stage_corpus.py \
  --profile burla-test \
  --dashboard-url http://localhost:<port> \
  --run-id govdocs1-v1
```

Process all indexed PDFs with a 2,000-worker ceiling:

```bash
BURLA_ENVIRONMENT=test uv run python run_burla.py \
  --profile burla-test \
  --dashboard-url http://localhost:<port> \
  --corpus-run-id govdocs1-v1 \
  --run-id govdocs1-text-v1 \
  --max-parallelism 2000
```

Interrupted runs save `results.partial.jsonl` and resume only missing document
IDs. Final records are `results.jsonl` and `summary.json`.

## August 16, 2026 full run

The unfiltered corpus contained 1,000 archives, 231,231 PDFs, and 137.31 GB of
uncompressed PDF members. The run produced:

- 221,106 text objects and 10,125 structured document failures
- 6,094,903 pages, including 196,424 OCR pages
- 13,781,512,383 text bytes
- complete one-to-one manifest coverage with no missing or extra document IDs

The main invocation, job `extract_text-Sun-nhN2TdW_`, returned 231,225 results
from 03:39:45 to 03:59:47 UTC before a pressure-retirement cleanup race failed
the job. Job `extract_text-ofKWlFo1Txa-` resumed the six missing documents and
completed 6/6 after the race fix.

The main invocation launched 45 EC2 instances totaling 2,572 vCPUs despite the
2,000-worker ceiling. During the three steady five-minute CloudWatch windows,
vCPU-weighted CPU utilization was 71.59%, 72.10%, and 67.75%. Only 19 to 22 of
45 nodes exceeded 90% CPU while 19 to 20 nodes remained below 50%. Network
traffic was 142.74 GiB inbound and 17.42 GiB outbound.

This run does not validate the intended all-but-one-node saturation goal or a
compute-efficiency advantage over Ray or Dask. Replacement and worker
re-add/retire oscillation must be fixed before running those baselines.
