# klite AWS Benchmark Infrastructure

Reproduce a klite vs MSK benchmark on AWS. Everything runs on Graviton arm64
instances in eu-west-1 using the default VPC.

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

- AWS CLI configured with profile `klite-bench` (IAM user with admin access, account `655506454434`)
- Terraform >= 1.5 (managed via tfenv; v1.12.2 known to work)
- Docker with buildx (for cross-compiling arm64 images from a non-arm host)
- SSH key — the public key content goes in `bench.tfvars`

## Quick Start (copy-paste)

This section has the exact commands to go from zero to a running benchmark.
Substitute the terraform output values where indicated.

```bash
# 1. Deploy infrastructure
cd infra/bench
terraform init   # only needed first time
terraform apply -var-file=bench.tfvars
# Note the outputs: klite_public_ip, klite_private_ip, bench_public_ip,
# ecr_klite, ecr_klite_bench, s3_bucket

# 2. Build and push images (from repo root)
cd ../..
aws ecr get-login-password --region eu-west-1 --profile klite-bench \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com

docker buildx build --platform linux/arm64 \
  -t 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite:latest \
  -f Dockerfile --push .

docker buildx build --platform linux/arm64 \
  -t 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  -f Dockerfile.bench --push .

# 3. ECR login on EC2 instances (userdata runs as root, ec2-user needs its own login)
ssh ec2-user@<klite_public_ip> 'aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com'

ssh ec2-user@<bench_public_ip> 'aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com'

# 4. Pull images
ssh ec2-user@<klite_public_ip> \
  'docker pull 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite:latest'
ssh ec2-user@<bench_public_ip> \
  'docker pull 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest'

# 5. Start klite (use --net host, NOT -p 9092:9092)
ssh ec2-user@<klite_public_ip> 'docker run -d --name klite \
  --net host -v /data/klite:/data \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite:latest \
  --data-dir /data \
  -advertised-addr <klite_private_ip>:9092 \
  -s3-bucket <s3_bucket> \
  -s3-region eu-west-1 \
  -s3-flush-interval 60s \
  -wal-max-disk-size 32212254720 \
  -chunk-pool-memory 4294967296'

# 6. Verify klite is running
ssh ec2-user@<klite_public_ip> 'docker logs klite'
# Should show: "klite started" with S3 storage initialized

# 7. Create topic + run 1-hour benchmark
ssh ec2-user@<bench_public_ip> 'docker run --rm --net host \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  create-topic -bootstrap-server <klite_private_ip>:9092 -topic bench-1h -partitions 6'

ssh ec2-user@<bench_public_ip> 'docker run -d --name bench-1h \
  --net host -v /tmp/results:/results \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  produce \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-1h \
  -num-records 360050000 \
  -record-size 1024 \
  -producers 4 \
  -acks -1 \
  -throughput 100000 \
  -warmup-records 50000 \
  -reporting-interval 60000 \
  -json-output /results/bench-1h.jsonl'

# 8. Monitor progress
ssh ec2-user@<bench_public_ip> 'docker logs --tail 5 bench-1h'

# 9. Download results when done
scp ec2-user@<bench_public_ip>:/tmp/results/bench-1h.jsonl tmp/

# 10. Tear down
cd infra/bench && terraform destroy -var-file=bench.tfvars
```

## Gotchas and Lessons Learned

### ECR login: userdata runs as root, ec2-user needs separate login

The userdata script runs `docker login` as root. When you SSH in as `ec2-user`
and try `docker pull`, you get `no basic auth credentials`. You must run ECR
login again as ec2-user:

```bash
aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com
```

### Use `--net host`, not `-p 9092:9092`

klite advertises the private IP to clients. With port-mapping (`-p 9092:9092`)
the client connects to the advertised private IP directly, bypassing Docker's
port mapping on the bench instance side. Using `--net host` on both containers
avoids all NAT issues. Use `--net host` on the bench container too.

### klite flags: `-advertised-addr` not `-advertise-addr`

Note the **d** at the end. The flag is `-advertised-addr`.

### The old `-ring-buffer-max-memory` flag is now `-chunk-pool-memory`

The ring buffer memory flag was renamed. The current flag is `-chunk-pool-memory`.
Default is 536870912 (512 MiB). For benchmarks, use 4294967296 (4 GiB) on
m7g.xlarge (16 GB RAM).

### Use `docker buildx build --push` for cross-platform builds

On a non-arm host, `docker build` then `docker push` may push the wrong
platform. Always use `docker buildx build --platform linux/arm64 --push`
to build and push in one step.

### Wait ~30s after terraform apply before SSHing

EC2 instances need time to boot and run userdata (install Docker, etc.).
Wait at least 30 seconds after `terraform apply` completes before attempting
SSH. Docker should be ready by then.

### Instance type: klite is m7g.xlarge, bench is m7g.large

klite needs 4 vCPU / 16 GB (m7g.xlarge) for sustained benchmarks with 4GB
chunk pool memory. The bench instance only runs the bench client and can be
m7g.large (2 vCPU / 8 GB).

## Detailed Steps

### 1. Deploy Infrastructure

```bash
cd infra/bench

# Create your tfvars (gitignored)
cp bench.tfvars.example bench.tfvars
# Edit bench.tfvars — set ssh_public_key to your public key contents
# Set klite_instance_type = "m7g.xlarge"

terraform init
terraform apply -var-file=bench.tfvars
```

This creates:
- SSH key pair
- Security group (SSH from anywhere, Kafka 9092 from VPC)
- 2 ECR repos (`klite`, `klite-bench`)
- IAM role with ECR pull + SSM + S3 access
- S3 bucket for klite data (`klite-bench-data-<random>`, force_destroy=true)
- EC2 klite instance (m7g.xlarge, gp3 100GB, 3000 IOPS, 125 MB/s throughput)
- EC2 bench instance (m7g.large, gp3 20GB)

Terraform outputs:
```
klite_public_ip   = "x.x.x.x"        # SSH access
klite_private_ip  = "172.31.x.x"      # bootstrap server for bench
bench_public_ip   = "y.y.y.y"         # SSH access
ecr_klite         = "655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite"
ecr_klite_bench   = "655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench"
s3_bucket         = "klite-bench-data-xxxxxxxx"
```

### 2. Build and Push Docker Images

From the repo root:

```bash
# ECR login (from your local machine)
aws ecr get-login-password --region eu-west-1 --profile klite-bench \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com

# Build and push both (use buildx for cross-platform arm64)
docker buildx build --platform linux/arm64 \
  -t 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite:latest \
  -f Dockerfile --push .

docker buildx build --platform linux/arm64 \
  -t 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  -f Dockerfile.bench --push .
```

### 3. Pull Images on EC2 Instances

```bash
# ECR login as ec2-user (required — see gotchas above)
ssh ec2-user@<klite_public_ip> 'aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com'
ssh ec2-user@<bench_public_ip> 'aws ecr get-login-password --region eu-west-1 \
  | docker login --username AWS --password-stdin 655506454434.dkr.ecr.eu-west-1.amazonaws.com'

# Pull
ssh ec2-user@<klite_public_ip> \
  'docker pull 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite:latest'
ssh ec2-user@<bench_public_ip> \
  'docker pull 655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest'
```

### 4. Start klite Broker

```bash
ssh ec2-user@<klite_public_ip> 'docker run -d --name klite \
  --net host \
  -v /data/klite:/data \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite:latest \
  --data-dir /data \
  -advertised-addr <klite_private_ip>:9092 \
  -s3-bucket <s3_bucket> \
  -s3-region eu-west-1 \
  -s3-flush-interval 60s \
  -wal-max-disk-size 32212254720 \
  -chunk-pool-memory 4294967296'
```

Key flags:
- `--net host` — required so clients can reach the advertised private IP
- `-advertised-addr` — must be the private IP so bench client can connect via VPC
- `-s3-flush-interval 60s` — flush WAL to S3 every 60 seconds
- `-wal-max-disk-size 32212254720` — 30GB WAL cap (disk is 100GB)
- `-chunk-pool-memory 4294967296` — 4GB chunk pool (m7g.xlarge has 16GB RAM)

Verify it's running:
```bash
ssh ec2-user@<klite_public_ip> 'docker logs klite'
# Should show: "klite started" with S3 storage initialized, flush_interval=1m0s
```

### 5. Run Benchmarks

All benchmark commands run from the **bench instance**. Always use `--net host`.

#### Create a topic

```bash
ssh ec2-user@<bench_public_ip> 'docker run --rm --net host \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  create-topic \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-test \
  -partitions 6'
```

#### Sustained-load produce benchmark (1 hour)

```bash
# 100K records/sec for 1 hour = 360M records + 50K warmup
# Report every 60 seconds for time-series plotting
ssh ec2-user@<bench_public_ip> 'docker run -d --name bench-1h \
  --net host -v /tmp/results:/results \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  produce \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-1h \
  -num-records 360050000 \
  -record-size 1024 \
  -producers 4 \
  -acks -1 \
  -throughput 100000 \
  -warmup-records 50000 \
  -reporting-interval 60000 \
  -json-output /results/bench-1h.jsonl'

# Monitor progress
ssh ec2-user@<bench_public_ip> 'docker logs --tail 5 bench-1h'
```

#### Sustained-load produce benchmark (6 hours)

```bash
# 100K/sec for 6 hours = 2.16B records + 50K warmup
ssh ec2-user@<bench_public_ip> 'docker run -d --name bench-6h \
  --net host -v /tmp/results:/results \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  produce \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-6h \
  -num-records 2160050000 \
  -record-size 1024 \
  -producers 4 \
  -acks -1 \
  -throughput 100000 \
  -warmup-records 50000 \
  -reporting-interval 60000 \
  -json-output /results/bench-6h.jsonl'
```

#### Max-throughput produce benchmark

```bash
ssh ec2-user@<bench_public_ip> 'docker run --rm --net host \
  -v /tmp/results:/results \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  produce \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-test \
  -num-records 1000000 \
  -record-size 1024 \
  -producers 4 \
  -acks -1 \
  -warmup-records 10000 \
  -json-output /results/produce-max.jsonl'
```

#### Consume benchmark

```bash
ssh ec2-user@<bench_public_ip> 'docker run --rm --net host \
  -v /tmp/results:/results \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  consume \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-test \
  -num-records 1000000 \
  -consumers 4 \
  -json-output /results/consume.jsonl'
```

#### Delete a topic

```bash
ssh ec2-user@<bench_public_ip> 'docker run --rm --net host \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  delete-topic \
  -bootstrap-server <klite_private_ip>:9092 \
  -topic bench-test'
```

## klite-bench CLI Reference

```
klite-bench produce [flags]        Run producer throughput/latency test
klite-bench consume [flags]        Run consumer throughput test
klite-bench create-topic [flags]   Create a topic
klite-bench delete-topic [flags]   Delete a topic
```

### produce flags

| Flag | Default | Description |
|------|---------|-------------|
| `-bootstrap-server` | `localhost:9092` | Broker address(es), comma-separated |
| `-topic` | `bench` | Topic to produce to |
| `-num-records` | `1000000` | Total records to produce |
| `-record-size` | `1000` | Record size in bytes |
| `-producers` | `1` | Number of concurrent producer clients |
| `-acks` | `-1` | Required acks: -1 (all/idempotent), 0, 1 |
| `-throughput` | `-1` | Records/sec cap (-1 = unlimited) |
| `-batch-max-bytes` | `1048576` | Max bytes per batch (1MB) |
| `-linger-ms` | `5` | Producer linger in milliseconds |
| `-max-buffered-records` | `8192` | Max records buffered in client (backpressure) |
| `-warmup-records` | `0` | Records to discard from stats |
| `-reporting-interval` | `5000` | Interval in ms between periodic reports |
| `-json-output` | | Path to JSON Lines time-series file |

### consume flags

| Flag | Default | Description |
|------|---------|-------------|
| `-bootstrap-server` | `localhost:9092` | Broker address(es), comma-separated |
| `-topic` | `bench` | Topic to consume from |
| `-num-records` | `1000000` | Records to consume before stopping |
| `-consumers` | `1` | Number of concurrent consumers |
| `-fetch-max-bytes` | `1048576` | Max fetch bytes per partition |
| `-timeout` | `60000` | Max ms between records before exit |
| `-reporting-interval` | `5000` | Interval in ms between periodic reports |
| `-json-output` | | Path to JSON results file |

## JSON Lines Output Format

When `-json-output` is specified, the produce command writes one JSON object
per line per reporting interval. This is designed for time-series plotting.

Each window line:
```json
{
  "ts": "2026-03-05T15:42:00Z",
  "elapsed_s": 60.0,
  "type": "window",
  "w_records": 1000000,
  "w_recs_sec": 100000.0,
  "w_mb_sec": 97.65,
  "w_avg_ms": 6.2,
  "w_p50_ms": 6,
  "w_p95_ms": 9,
  "w_p99_ms": 10,
  "w_p999_ms": 16,
  "w_max_ms": 35,
  "c_records": 6000000,
  "c_recs_sec": 100000.0,
  "c_mb_sec": 97.65,
  "c_avg_ms": 6.3,
  "c_max_ms": 35
}
```

Final summary line (type="final") is written at the end.

### Plotting with Python

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_json("produce.jsonl", lines=True)
df = df[df["type"] == "window"]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

# Throughput over time
ax1.plot(df["elapsed_s"] / 3600, df["w_recs_sec"] / 1000, label="records/sec (K)")
ax1.set_ylabel("Throughput (K records/sec)")
ax1.legend()

# Latency percentiles over time
ax2.plot(df["elapsed_s"] / 3600, df["w_p50_ms"], label="p50")
ax2.plot(df["elapsed_s"] / 3600, df["w_p95_ms"], label="p95")
ax2.plot(df["elapsed_s"] / 3600, df["w_p99_ms"], label="p99")
ax2.plot(df["elapsed_s"] / 3600, df["w_p999_ms"], label="p99.9")
ax2.set_xlabel("Time (hours)")
ax2.set_ylabel("Latency (ms)")
ax2.legend()

plt.tight_layout()
plt.savefig("latency-over-time.png", dpi=150)
```

## Operational Notes

### Warmup and num-records

The `-warmup-records` count is included in `-num-records`. To get exactly N
minutes of measured data at rate R, set `num-records = R * N * 60 + warmup`.

| Duration | Rate | Warmup | num-records |
|----------|------|--------|-------------|
| 5 min | 100K/sec | 50K | 30,050,000 |
| 1 hour | 100K/sec | 50K | 360,050,000 |
| 6 hours | 100K/sec | 50K | 2,160,050,000 |

### Clean start between benchmarks

Always start klite fresh between benchmark runs to avoid leftover WAL data
and S3 objects from prior runs contaminating results:

```bash
# On klite instance
ssh ec2-user@<klite_public_ip> 'docker rm -f klite && sudo rm -rf /data/klite/*'

# Clean S3 bucket (from local machine)
aws s3 rm s3://<s3_bucket>/ --recursive --profile klite-bench

# Then start klite again with docker run ...
```

### Topic lifecycle

Topics must be explicitly created before and deleted after each benchmark.
If `create-topic` reports `TOPIC_ALREADY_EXISTS`, use a fresh topic name.

### Verifying broker health after a run

After a benchmark, check klite logs for warnings:

```bash
ssh ec2-user@<klite_public_ip> 'docker logs klite 2>&1 | grep -iE "error|warn|fail|emergency|force-del"'
```

Key things to look for:
- `force-deleted WAL segment (disk limit exceeded)` — WAL segments deleted
  before S3 flush, indicating data loss
- `emergency flush triggered by WAL pressure` — flusher detected WAL pressure
- `S3 partition flush failed` — S3 upload errors

### Downloading results

```bash
# From your local machine
scp ec2-user@<bench_public_ip>:/tmp/results/bench-1h.jsonl tmp/
```

## Enable MSK (for comparison)

```bash
# Edit bench.tfvars
enable_msk = true

# Apply (takes ~20 minutes, costs ~$0.72/hr)
terraform apply -var-file=bench.tfvars
```

Then run the same benchmarks against the MSK bootstrap servers:

```bash
ssh ec2-user@<bench_public_ip> 'docker run --rm --net host \
  -v /tmp/results:/results \
  655506454434.dkr.ecr.eu-west-1.amazonaws.com/klite-bench:latest \
  produce \
  -bootstrap-server <msk_bootstrap_plaintext> \
  -topic bench-test \
  ...'
```

## Tear Down

```bash
# Destroy all AWS resources (including S3 bucket and contents)
cd infra/bench
terraform destroy -var-file=bench.tfvars
```

## Cost Estimate

| Resource | Cost |
|----------|------|
| m7g.xlarge EC2 (klite) | ~$0.15/hr |
| m7g.large EC2 (bench) | ~$0.08/hr |
| gp3 100GB + 20GB | ~$0.01/hr |
| S3 | negligible |
| MSK 3x kafka.m7g.large (if enabled) | ~$0.72/hr |
| **Total without MSK** | **~$0.24/hr** |
| **Total with MSK** | **~$0.96/hr** |

Remember to `terraform destroy` when done.
