# Adaptive concurrency benchmarks (2026-08-28 to 2026-09-01)

These are historical measured controller runs, not the current landing-page
values. The current mix of measurements and projections is documented in
[`examples/adaptive-concurrency-raydask/results/README.md`](../../examples/adaptive-concurrency-raydask/results/README.md).

Provenance record for the controller work on branch `cursor/6ea4fdc5`
(damping, saturation creep, proportional minting, memory-PSI gating).
Full artifacts (`results.jsonl`, `summary.json`, derived outputs) for every
run live in `s3://burla-adaptive-benchmarks-002645521087/runs/<workload>/<run-id>/`
(AWS profile `burla-test`). Job debug trails are in the `cluster-l0ch` head
state dir (`debug_logs` / `resource_metrics` tables) keyed by the job ids
below. All runs: 32x m7i.16xlarge asked (2,048 vCPUs), us-east-1; grow=True
fleets settled at 40 nodes on NEXRAD/ABO for both baselines and reruns, so
comparisons are like-for-like.

"Flips" = `workers_throttled` + `worker_unthrottled` debug events.

## Outcome-controller validation (2026-09-01)

These runs validate the head-side marginal-goodput controller after removing
the arbitrary per-core mint ceiling, and the immediate worker recovery path
after removing the recovery dwell.

| workload | final run / job | result | comparison |
|---|---|---|---|
| GovDocs PDF OCR | `govdocs1-burla-recoveryfinal4-20260901T093113Z` / `extract_text-I0ae2Z32QW6I` | 1404.4s, 164.6 docs/s, 620.5 used CPU-hours, 10,132 document failures | Within 1.8% of the 23.0-minute best; bulk CPU averaged 98.7% (95.6-100.0% by minute). Immediate recovery handled 19,584 parks without leaving CPU idle. |
| Smithsonian | `smithsonian-burla-recoveryfinal5-20260901T111212Z` / `process_smithsonian_asset-6Ugr4q68RIaS` | 2304.2s, 868.0/s, 581.2 used CPU-hours, 1,684 failures | 4.3% slower than the 2208.8s baseline, with 3,073 steady workers instead of 11,169 (72% fewer) and 98.5% fewer failures. The controller verified 2,048 to 2,253 workers, retained one non-harmful 820-worker step, then rejected probes without ratcheting. |
| NEXRAD | `nexrad-burla-mintctl-final-20260901T120759Z` / `derive_nexrad_reflectivity-ypXMXG4ATVKW` | 1110.4s, 607.3/s, 656.6 used CPU-hours, 41 failed | 3.4% slower than the 1074.0s best; warm CPU averaged 93.0% and stayed at 92.2-94.8% late in the run. There was one park/unpark pair. Two 40-worker probes were fully shed with no visible CPU or throughput oscillation. |

Final artifacts:

- GovDocs: `s3://burla-govdocs1-corpus-002645521087/runs/govdocs1-burla-recoveryfinal4-20260901T093113Z/`
- Smithsonian: `s3://burla-adaptive-benchmarks-002645521087/runs/smithsonian/smithsonian-burla-recoveryfinal5-20260901T111212Z/`
- NEXRAD: `s3://burla-adaptive-benchmarks-002645521087/runs/nexrad/nexrad-burla-mintctl-final-20260901T120759Z/`

Supporting memory-bound validation: `pd12m-burla-mintctl2-20260901T051647Z`
completed 2M images in 716.8s (4.1% slower than the 688.6s memory-gate best)
with 194 failures instead of 1,861.

### Outcome-controller flaws found and resolved

- An allowance captured during the boot ramp stayed tiny after an early
  no-evidence close, permanently preventing useful experiments. Allowance now
  rebases from the current fleet size.
- Startup completion bursts polluted the noise estimate and hid real gains.
  Epochs now wait for eight full-load blocks and estimate noise from the latest
  four stable blocks.
- A zero-goodput epoch with zero estimated noise passed `0 >= 0` and could
  verify without evidence. Verification now requires gain strictly above the
  noise floor.
- Letting one node request repeatedly reused the same stale headroom sample.
  Each node may now contribute only one locally sized batch per epoch.
- Keeping every rejected experiment caused worker-count ratcheting, but
  reclaiming every low-value step made Smithsonian 13% slower by discarding
  useful overlap. Harmful steps and failed probes are reclaimed; a non-harmful
  final step is retained while further growth freezes.
- A 15-120s recovery dwell restarted after each park and held GovDocs at
  70-87% CPU. Removing it restored immediate recovery and 98.7% bulk CPU.
- A node could report drained while a peer input GET/ACK was still in flight,
  stranding inputs after relay failures. Drain claims now wait for the active
  steal transaction.
- The client upload retry omitted `ClientOSError`, so a broken pipe aborted
  otherwise recoverable input uploads. It now retries every recorded network
  error type.
- GovDocs used a thread watchdog that could not kill blocked native OCR
  subprocesses. Each document now runs in its own process group with a hard
  kill after the observed 900-second budget.
- The NEXRAD harness still capped itself at 2,048 vCPUs while the comparison
  required 40 nodes. It now uses 2,560 vCPUs and records the actual fleet.
- NEXRAD completed, but its final metrics query transiently exceeded the CLI's
  30-second timeout and skipped artifact upload. The completed local results
  were summarized and uploaded without rerunning; the harness now retries a
  recorded retryable metrics failure once.

## NEXRAD Level II reflectivity (674,338 items, download 7-20MB then CPU-parse)

| run | run id / job id | workload | events | notes |
|---|---|---|---|---|
| baseline (dev) | `nexrad-burla-dev-full-20260828T0536Z` / `derive_nexrad_reflectivity-w2KD63XdRLyl` | 1362.5s, 494.9/s | 1,369 flips, 44 mints, 40 replacement 409s | 8 min of fleet CPU at 40-69%; 9 failed |
| damper | `nexrad-burla-damper-full-20260829T001931Z` / `derive_nexrad_reflectivity-KD_3jng9Tp2d` | 1102.1s, 611.9/s | 0 flips, 0 mints, 0 replacements | 92.3-93.1% CPU every steady minute; same 9 failed ids |
| saturation creep | `nexrad-burla-saturate-full-20260830T061929Z` / `derive_nexrad_reflectivity-uf4uV7DmSLua` | 1229.0s, 548.7/s | 137 flips, 124 mints | 98.3-98.4% CPU, ~67 slots/64 cores; tail ate a 180s hung upload |
| final (upload budget fix) | `nexrad-burla-uploadfix-full-20260830T070532Z` / `derive_nexrad_reflectivity-1NJvDvaSQA6S` | **1074.0s, 627.9/s (-21% vs baseline)** | 18 flips, 124 mints, 1 replacement 409 | ~98% steady CPU; same 9 deterministic parse failures as baseline |

## Amazon Berkeley Objects derivatives (979,998 items, image decode + 11 renditions)

| run | run id / job id | workload | failed | notes |
|---|---|---|---|---|
| baseline (dev, 60s budget) | `abo-burla-dev-60s-20260828T0530Z` / `process_amazon_berkeley_object-6Wh1t5zJRouq` | 812.1s, 1206.8/s | 83 | 63 flips, 0 mints |
| damper | `abo-burla-damper-60s-20260829T005126Z` / `process_amazon_berkeley_object-27gzSolsSOSz` | 814.1s, 1203.7/s | 73 | 52 flips, 0 mints; 88.5% steady CPU |
| final (saturation creep) | `abo-burla-saturate-60s-20260830T064534Z` / `process_amazon_berkeley_object-quFdYPYVQwyv` | **797.5s, 1228.9/s** | 66 | 94.7% steady CPU (peak 97.7) |

## Common Crawl text extraction (1,072,733 WARC records, range-fetch + HTML parse)

| run | run id / job id | workload | notes |
|---|---|---|---|
| baseline (1.7.11) | `commoncrawl-burla-1711-20260828T0627Z` | 250.5s, 4281.8/s | 0 failed |
| final (proportional mint) | `commoncrawl-burla-mintfix2-20260830T200058Z` / `extract_common_crawl_page-KBrqnaF7QXO_` | **239.2s, 4485.0/s (-4.5%)** | 0 failed, zero controller events |

Intermediate runs `commoncrawl-burla-damper-20260830T184330Z` (canceled: first
damper build ramped too slowly for a 4-minute job), `-damper-20260830T191111Z`,
and `-mintfix-20260830T193639Z` are in the same S3 prefix.

## Smithsonian Open Access derivatives (2,000,000 items)

| run | run id / job id | workload | failed | notes |
|---|---|---|---|---|
| first attempts | `smithsonian-burla-damper-20260830T202221Z` + tricklefix reruns | canceled at 66 and 77 min | - | tail hung on trickle downloads (bytes dribbling forever never trip per-read timeouts) |
| final (watchdog + trickle-proof budgets) | `smithsonian-burla-watchdog-20260830T230327Z` / `process_smithsonian_asset-tg67zWH3TmGR` | **2208.8s, 905.5/s** | 113,211 (5.7%: 108,922 download_timeout, 4,229 unexpected, 60 decode) | 374 mints, 0 parks; failures are Smithsonian CDN rate limiting at this concurrency, not cluster faults |

## PD12M image derivatives (2,000,000 items) - Burla vs Ray vs Dask

Same workload, same account, same 2,048-2,560 vCPU scale. Harness:
`examples/adaptive-concurrency-raydask/`.

| framework | run id | workload | failed |
|---|---|---|---|
| **Burla (final, memory-PSI gate)** | `pd12m-burla-memgate-20260831T035853Z` / `process_pd12m_image-zRCE9ys9TqiQ` | **688.6s, 2904.3/s** | 1,861 (0.09%) |
| Ray | `pd12m-ray-20260830T221225Z` | 3596.8s (5.2x slower) | 4 |
| Dask | `pd12m-dask-20260830T232945Z` | 5187.0s (7.5x slower) | 8 |

Burla iterations on the way to the final number: `pd12m-burla-20260830T234619Z`
(12.7 min), `pd12m-burla-fastmint-20260831T001613Z` (16.5 min), three
`pd12m-burla-batchboot-*` attempts (two failed, three canceled while
diagnosing), then `pd12m-burla-memgate-20260831T035853Z` (11.5 min).

## Bugs found along the way (besides the controller itself)

- Trade-to-zero steal race: a node could accept stolen inputs after trading
  away its last slot, stranding them with no workers and hanging the job;
  fixed in `36b8864` (steal serialized against trades under the retire lock,
  plus a stranded-input target reclaim).
- Hung S3 uploads in the benchmark workers: 180s per-attempt timeout, 12
  attempts, no total budget, so one hung POST sat 3 minutes on a job tail;
  fixed in `69d77df` with a hard 60s total budget mirroring downloads.
- Trickle downloads in the benchmark workers: connections dripping bytes never
  trip per-read timeouts and hung tails for an hour-plus; fixed in `959bea2`
  and `43ec7c9` with trickle-proof total budgets.
- Assignment 502 flake (open): freshly booted nodes intermittently answer 502
  through the relay during job assignment and the client dies despite
  retries; killed two runs on 2026-08-29, not yet diagnosed.
- NEXRAD summary metadata (open, cosmetic): `worker_nodes` records the CLI
  flag while the fleet actually ran 40 nodes; the ABO runner measures it from
  job metrics instead.
