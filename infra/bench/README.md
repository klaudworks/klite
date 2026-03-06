# klite AWS Benchmark Infrastructure

Reproduce a klite vs MSK benchmark on AWS. Everything runs on Graviton arm64
spot instances in eu-west-1 using the default VPC.

All orchestration is handled by a single script: `scripts/bench-aws.py`.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Default VPC (172.31.0.0/16) — eu-west-1            │
│                                                     │
│  ┌──────────────┐         ┌──────────────┐          │
│  │ klite EC2    │  :9092  │ bench EC2    │          │
│  │ m7g.xlarge   │◄────────│ m7g.large    │          │
│  │ gp3 100GB   │         │ gp3 20GB     │          │
│  │ 3000 IOPS   │         │              │          │
│  │ 125 MB/s    │         │ klite-bench  │          │
│  └──────┬───────┘         └──────────────┘          │
│         │                                           │
│         ▼                                           │
│  ┌──────────────┐                                   │
│  │ S3 bucket    │                                   │
│  │ klite data   │                                   │
│  └──────────────┘                                   │
│                                                     │
│  (optional, enable_msk=true)                        │
│  ┌──────────────┐                                   │
│  │ MSK cluster  │                                   │
│  │ 3x kafka.m7g │                                   │
│  │ .large       │                                   │
│  └──────────────┘                                   │
└─────────────────────────────────────────────────────┘
```

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package runner)
- AWS CLI configured with profile `klite-bench`
- Terraform >= 1.5
- Docker with buildx (for cross-compiling arm64 images)
- SSH key pair (the script auto-detects `~/.ssh/id_ed25519.pub`)

## Setup

Edit `infra/bench/bench.tfvars` if you need to change region, profile, or
instance types. The file is checked into git (no secrets).

The SSH key is auto-detected from `~/.ssh/id_ed25519.pub` (falling back to
`~/.ssh/id_rsa.pub`). To use a different key:

```bash
./scripts/bench-aws.py up --ssh-key ~/.ssh/my_key.pub
```

## Quick Start

```bash
# Bring up infra, build images, push to ECR, pull on instances
./scripts/bench-aws.py up

# Run a 1-hour benchmark with defaults
./scripts/bench-aws.py run

# Check progress
./scripts/bench-aws.py status

# Tear down when done
./scripts/bench-aws.py down
```

Results are saved to `tmp/<timestamp>-<mode>.jsonl`.

## Commands

### `up` — Provision infrastructure and deploy images

```bash
./scripts/bench-aws.py up
./scripts/bench-aws.py up --ssh-key ~/.ssh/my_key.pub
```

Runs terraform apply, waits for instances to boot, builds both Docker images
for linux/arm64, pushes to ECR, and pulls on the EC2 instances. Handles
terraform init automatically on first run. The SSH public key is auto-detected
from `~/.ssh/id_ed25519.pub` unless overridden with `--ssh-key`.

### `push` — Rebuild and redeploy images

```bash
./scripts/bench-aws.py push
```

Rebuilds both images and pushes them to the instances without touching
terraform. Use after code changes between benchmark runs.

### `run` — Execute a benchmark

```bash
./scripts/bench-aws.py run [OPTIONS]
```

Each run gets a unique ID (timestamp) used for the WAL data directory, S3
prefix, and output filename. Runs are fully isolated — no need to clean S3
between runs.

| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | `produce-consume` | `produce-consume`, `produce`, or `consume` |
| `--topic` | `bench` | Topic name |
| `--partitions` | `6` | Partition count |
| `--num-records` | `360050000` | Total records (1h at 100K/s + 50K warmup) |
| `--record-size` | `1024` | Bytes per record |
| `--producers` | `4` | Producer count |
| `--consumers` | `4` | Consumer count |
| `--acks` | `1` | Required acks: -1, 0, 1 |
| `--throughput` | `100000` | Records/sec cap (-1 = unlimited) |
| `--warmup-records` | `50000` | Warmup records |
| `--reporting-interval` | `60000` | Report interval in ms |
| `--label` | | Label appended to output filename |

### `down` — Tear down infrastructure

```bash
./scripts/bench-aws.py down
```

### `status` — Check benchmark progress

```bash
./scripts/bench-aws.py status
```

Shows container status and recent logs on both instances.

### `ssh-klite` / `ssh-bench` — Interactive SSH

```bash
./scripts/bench-aws.py ssh-klite
./scripts/bench-aws.py ssh-bench
```

## Benchmark Recipes

### 1-hour produce-consume (default)

```bash
./scripts/bench-aws.py run
```

100K rec/s, 4 producers, 4 consumers, acks=1, 1KB records, 6 partitions.

### 6-hour endurance

```bash
./scripts/bench-aws.py run --num-records 2160050000 --label 6h
```

### Max-throughput produce

```bash
./scripts/bench-aws.py run --mode produce --throughput -1 --num-records 1000000 --label max-throughput
```

### Multiple runs with different settings

```bash
./scripts/bench-aws.py up

./scripts/bench-aws.py run --label acks1
./scripts/bench-aws.py run --acks -1 --label acks-all
./scripts/bench-aws.py run --throughput -1 --num-records 5000000 --label burst

./scripts/bench-aws.py down
```

Each run produces a separate timestamped file in `tmp/`.

## Warmup and num-records

Warmup is included in num-records: `num-records = rate * seconds + warmup`.

| Duration | Rate | Warmup | num-records |
|----------|------|--------|-------------|
| 5 min | 100K/sec | 50K | 30,050,000 |
| 1 hour | 100K/sec | 50K | 360,050,000 |
| 6 hours | 100K/sec | 50K | 2,160,050,000 |

## JSONL Output Format

Each reporting interval emits one JSON object (type=`window`). A final summary
(type=`final`) is written at the end.

```json
{"ts":"2026-03-05T15:42:00Z","elapsed_s":60.0,"type":"window","w_records":1000000,"w_recs_sec":100000.0,"w_mb_sec":97.65,"w_avg_ms":6.2,"w_p50_ms":6,"w_p95_ms":9,"w_p99_ms":10,"w_p999_ms":16,"w_max_ms":35,"c_records":6000000,"c_recs_sec":100000.0,"c_mb_sec":97.65,"c_avg_ms":6.3,"c_max_ms":35}
```

### Plotting

```python
import pandas as pd, matplotlib.pyplot as plt

df = pd.read_json("results.jsonl", lines=True)
df = df[df["type"] == "window"]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
ax1.plot(df["elapsed_s"] / 3600, df["w_recs_sec"] / 1000)
ax1.set_ylabel("Throughput (K rec/sec)")
for p in ["w_p50_ms", "w_p95_ms", "w_p99_ms", "w_p999_ms"]:
    ax2.plot(df["elapsed_s"] / 3600, df[p], label=p.replace("w_", "").replace("_ms", ""))
ax2.set_xlabel("Time (hours)"); ax2.set_ylabel("Latency (ms)"); ax2.legend()
plt.tight_layout(); plt.savefig("latency-over-time.png", dpi=150)
```

## MSK Comparison

To benchmark against MSK instead of klite, enable the MSK cluster in
`bench.tfvars` and SSH into the bench instance to run the bench container
directly against the MSK bootstrap servers.

```bash
# In bench.tfvars: enable_msk = true
./scripts/bench-aws.py up   # ~20 min extra for MSK provisioning

# Get the MSK bootstrap servers
cd infra/bench && terraform output msk_bootstrap_plaintext

# SSH in and run benchmarks manually against MSK
./scripts/bench-aws.py ssh-bench
```

## Terraform Details

The terraform configuration in `infra/bench/` creates:

- SSH key pair, security group
- 2 ECR repositories (klite, klite-bench)
- IAM role with ECR pull + SSM + S3 access
- S3 bucket (`klite-bench-data-<random>`, force_destroy=true)
- EC2 klite instance (spot by default, gp3 100GB / 3000 IOPS / 125 MB/s)
- EC2 bench instance (spot by default, gp3 20GB)

Spot instances are enabled by default (`use_spot = true` in variables.tf).
Set `use_spot = false` in `bench.tfvars` to use on-demand instances.

## Troubleshooting

**SSH connection refused right after `up`**: The userdata script installs
Docker on boot. The script waits up to 120s for SSH, but if Docker isn't
ready yet, use `./scripts/bench-aws.py status` to check.

**Benchmark exits immediately**: Check klite logs with
`./scripts/bench-aws.py ssh-klite` then `docker logs klite`. Look for
`force-deleted WAL segment` (data loss), `emergency flush` (WAL pressure),
or `S3 partition flush failed` (upload errors).

**ECR login failures**: Verify your AWS profile is configured:
`aws sts get-caller-identity --profile klite-bench`

## Cost

| Resource | $/hr (on-demand) | $/hr (spot, typical) |
|----------|------------------|----------------------|
| m7g.xlarge (klite) | ~0.15 | ~0.06 |
| m7g.large (bench) | ~0.08 | ~0.03 |
| gp3 120GB total | ~0.01 | ~0.01 |
| MSK 3x kafka.m7g.large | ~0.72 | ~0.72 |
| **Without MSK** | **~0.24** | **~0.10** |
| **With MSK** | **~0.96** | **~0.82** |
