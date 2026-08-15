# PDF OCR benchmark

This benchmark turns real GovDocs1 PDFs into searchable PDF documents with
OCRmyPDF. Each input produces:

- `searchable.pdf`, with an OCR text layer
- `text.txt`, containing the extracted text
- `metadata.json`, containing checksums, byte counts, page count, timing, and
  the worker container that processed it

Inputs and outputs live in the private
`s3://burla-pdf-ocr-benchmark-002645521087` bucket. Workers use presigned URLs,
so the benchmark does not grant them AWS credentials.

The runner uses the repository's Burla 1.6.6 checkout and an immutable,
publicly pullable OCRmyPDF image with Python 3.12:

```text
public.ecr.aws/e2g5l2y4/burla-pdf-ocr-benchmark@sha256:924b5e226663f2f6f8df96f15dee75697eb6a8a7bae2314b5553b8039f09d7c8
```

## Stage the calibration corpus

From this directory:

```bash
uv run python stage_corpus.py \
  --profile burla-test \
  --count 32 \
  --manifest-name calibration-v1
```

This selects deterministic file-size quantiles from the 5,000-file sample in
`BEE-spoke-data/govdocs1-pdf-source`, validates each PDF, uploads it, and writes
`manifests/calibration-v1.json`.

## Run on Burla

Start a real EC2 cluster from the repository root:

```bash
AWS_PROFILE=burla-test DISABLE_BURLA_TELEMETRY=True make remote-dev
make cluster-info
```

Then run the calibration from this directory using the dashboard URL printed
above:

```bash
BURLA_ENVIRONMENT=test uv run python run_burla.py \
  --profile burla-test \
  --dashboard-url http://localhost:<port> \
  --manifest-key manifests/calibration-v1.json \
  --max-parallelism 8
```

The combined run record is written to `runs/<run-id>/summary.json`.

## Full-scale run

The production comparison will use 10,000 GovDocs1 PDFs selected from the
100-300 page range, totaling about 1.6 million pages. Run it with
`--max-parallelism 2000`; Burla packs that into 31 64-vCPU `m7i.16xlarge`
machines and one 16-vCPU `m7i.4xlarge`, staying below its 2,560-vCPU grow
limit. Stage and calibrate the full manifest before launching because the
current calibration command intentionally uses only the readily downloadable
5,000-file sample.
