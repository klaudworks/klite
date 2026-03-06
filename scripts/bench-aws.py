#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["typer>=0.15", "rich>=13"]
# ///
"""
bench-aws.py — Orchestrate klite AWS benchmarks.

Subcommands:
    up        Provision infrastructure, build & push images
    push      Rebuild & push images (skip terraform)
    run       Start klite, run benchmark, poll, fetch results
    down      Tear down infrastructure
    status    Show benchmark progress
    ssh-klite Open SSH to klite instance
    ssh-bench Open SSH to bench instance
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, NoReturn

import typer
from rich.console import Console

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
TF_DIR = REPO_ROOT / "infra" / "bench"
TFVARS = TF_DIR / "bench.tfvars"
RESULTS_DIR = REPO_ROOT / "tmp"

DEFAULT_SSH_KEY_PATHS = [
    Path.home() / ".ssh" / "id_ed25519.pub",
    Path.home() / ".ssh" / "id_rsa.pub",
]

SSH_OPTS = [
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=30",
    "-o", "LogLevel=ERROR",
]

# klite server-side flags (stable across benchmark runs)
KLITE_SERVER_FLAGS = {
    "s3-flush-interval": "60s",
    "wal-max-disk-size": "32212254720",   # 30 GiB
    "chunk-pool-memory": "4294967296",    # 4 GiB
}

console = Console()
app = typer.Typer(
    help="Orchestrate klite AWS benchmarks.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def die(msg: str) -> NoReturn:
    console.print(f"[red bold]Error:[/] {msg}")
    raise typer.Exit(1)


def step(n: int, total: int, msg: str) -> None:
    console.print(f"[cyan][{n}/{total}][/] {msg}")


def ok(msg: str) -> None:
    console.print(f"  [green]OK[/] {msg}")


def run_local(
    cmd: list[str],
    *,
    check: bool = True,
    capture: bool = False,
    cwd: Path | None = None,
    env: dict | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a local command, streaming output unless capture=True."""
    merged_env = {**os.environ, **(env or {})}
    try:
        return subprocess.run(
            cmd,
            check=check,
            cwd=cwd,
            capture_output=capture,
            text=True,
            env=merged_env,
        )
    except FileNotFoundError:
        die(f"Command not found: {cmd[0]}")
    except subprocess.CalledProcessError as e:
        detail = e.stderr.strip() if e.stderr else "(no stderr)"
        die(f"Command failed (exit {e.returncode}): {' '.join(cmd)}\n  {detail}")
    raise AssertionError("unreachable")  # die() always raises


def ssh_cmd(host: str) -> list[str]:
    return ["ssh", *SSH_OPTS, f"ec2-user@{host}"]


def ssh(host: str, command: str, *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command on a remote host via SSH."""
    return run_local([*ssh_cmd(host), command], check=check, capture=capture)


def scp(host: str, remote_path: str, local_path: Path) -> None:
    run_local(["scp", *SSH_OPTS, f"ec2-user@{host}:{remote_path}", str(local_path)])


# ---------------------------------------------------------------------------
# Tfvars / terraform helpers
# ---------------------------------------------------------------------------

def read_tfvars() -> dict[str, str]:
    """Parse bench.tfvars into a dict (simple key = "value" format)."""
    if not TFVARS.exists():
        die(f"Missing {TFVARS}\nCopy bench.tfvars.example and fill in your values.")
    result = {}
    for line in TFVARS.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r'(\w+)\s*=\s*"?([^"]*)"?', line)
        if m:
            result[m.group(1)] = m.group(2).strip()
    return result


def tf_output() -> dict[str, str]:
    """Read terraform outputs as a flat dict."""
    r = run_local(
        ["terraform", "output", "-json"],
        cwd=TF_DIR, capture=True,
    )
    raw = json.loads(r.stdout)
    if not raw:
        die("No terraform outputs found. Run './scripts/bench-aws.py up' first.")
    result = {k: v["value"] for k, v in raw.items()}
    # Verify essential outputs exist
    for key in ("klite_public_ip", "klite_private_ip", "bench_public_ip", "s3_bucket"):
        if not result.get(key):
            die(f"Terraform output '{key}' is empty. Is the infrastructure up?")
    return result


def get_aws_profile() -> str:
    return read_tfvars().get("aws_profile", "klite-bench")


def get_region() -> str:
    return read_tfvars().get("region", "eu-west-1")


def get_account_id() -> str:
    profile = get_aws_profile()
    r = run_local(
        ["aws", "sts", "get-caller-identity",
         "--profile", profile, "--query", "Account", "--output", "text"],
        capture=True,
    )
    return r.stdout.strip()


def ecr_registry(account_id: str, region: str) -> str:
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com"


def resolve_ssh_pubkey(ssh_key: str) -> str:
    """Resolve an SSH public key: read from file path, or return as-is if it looks like a key."""
    if ssh_key.startswith("ssh-"):
        return ssh_key
    path = Path(ssh_key).expanduser()
    if not path.exists():
        die(f"SSH key file not found: {path}")
    content = path.read_text().strip()
    if not content.startswith("ssh-"):
        die(f"File does not look like an SSH public key: {path}")
    return content


def find_default_ssh_pubkey() -> str:
    """Find the default SSH public key from well-known paths."""
    for p in DEFAULT_SSH_KEY_PATHS:
        if p.exists():
            return p.read_text().strip()
    names = ", ".join(p.name for p in DEFAULT_SSH_KEY_PATHS)
    die(
        f"No SSH public key found (tried {names} in ~/.ssh/).\n"
        f"  Specify one with: ./scripts/bench-aws.py up --ssh-key ~/.ssh/your_key.pub"
    )


def get_ssh_pubkey(ssh_key: str) -> str:
    """Get the SSH public key content from a flag value or auto-detect."""
    if ssh_key:
        return resolve_ssh_pubkey(ssh_key)
    return find_default_ssh_pubkey()


def wait_for_ssh(host: str, label: str, timeout: int = 120) -> None:
    """Poll until SSH succeeds on host."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = run_local(
            [*ssh_cmd(host), "true"],
            check=False, capture=True,
        )
        if r.returncode == 0:
            ok(f"{label} ({host}) is reachable")
            return
        time.sleep(5)
    die(f"SSH to {label} ({host}) timed out after {timeout}s. Is the instance running?")


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

@app.command()
def up(
    ssh_key: Annotated[str, typer.Option(
        help="SSH public key file or literal key. Default: ~/.ssh/id_ed25519.pub",
    )] = "",
) -> None:
    """Provision infrastructure, build & push Docker images."""
    total = 7

    pubkey = get_ssh_pubkey(ssh_key)

    # 1. Terraform init
    step(1, total, "Initializing terraform...")
    if not (TF_DIR / ".terraform").exists():
        run_local(["terraform", "init"], cwd=TF_DIR)
        ok("terraform init complete")
    else:
        ok("already initialized")

    # 2. Terraform apply
    step(2, total, "Applying infrastructure...")
    run_local(
        ["terraform", "apply", "-var-file=bench.tfvars",
         f"-var=ssh_public_key={pubkey}", "-auto-approve"],
        cwd=TF_DIR,
    )
    ok("infrastructure provisioned")

    # 3. Read outputs and wait for instances
    outputs = tf_output()
    klite_ip = outputs["klite_public_ip"]
    bench_ip = outputs["bench_public_ip"]

    step(3, total, "Waiting for instances to accept SSH (userdata installs docker)...")
    wait_for_ssh(klite_ip, "klite")
    wait_for_ssh(bench_ip, "bench")

    # 4. ECR login locally
    step(4, total, "Logging in to ECR...")
    region = get_region()
    profile = get_aws_profile()
    account_id = get_account_id()
    registry = ecr_registry(account_id, region)

    _ecr_login_local(region, profile, registry)
    ok(f"logged in to {registry}")

    # 5. Build klite image
    step(5, total, "Building klite image (linux/arm64)...")
    klite_tag = f"{registry}/klite:latest"
    run_local([
        "docker", "buildx", "build", "--platform", "linux/arm64",
        "-t", klite_tag, "-f", "Dockerfile", "--push", ".",
    ], cwd=REPO_ROOT)
    ok("klite image pushed")

    # 6. Build bench image
    step(6, total, "Building klite-bench image (linux/arm64)...")
    bench_tag = f"{registry}/klite-bench:latest"
    run_local([
        "docker", "buildx", "build", "--platform", "linux/arm64",
        "-t", bench_tag, "-f", "Dockerfile.bench", "--push", ".",
    ], cwd=REPO_ROOT)
    ok("klite-bench image pushed")

    # 7. Pull images on instances
    step(7, total, "Pulling images on EC2 instances...")
    ecr_login_and_pull = (
        f"aws ecr get-login-password --region {region}"
        f" | docker login --username AWS --password-stdin {registry}"
    )
    ssh(klite_ip, f"{ecr_login_and_pull} && docker pull {klite_tag}")
    ok("klite image pulled on klite instance")
    ssh(bench_ip, f"{ecr_login_and_pull} && docker pull {bench_tag}")
    ok("klite-bench image pulled on bench instance")

    console.print("\n[green bold]Infrastructure is ready.[/]")
    console.print(f"  klite:  ssh ec2-user@{klite_ip}")
    console.print(f"  bench:  ssh ec2-user@{bench_ip}")
    console.print(f"  bucket: {outputs['s3_bucket']}")
    console.print("\nRun a benchmark with: [bold]./scripts/bench-aws.py run[/]")


@app.command()
def push() -> None:
    """Rebuild and push Docker images (skip terraform)."""
    total = 4
    outputs = tf_output()
    klite_ip = outputs["klite_public_ip"]
    bench_ip = outputs["bench_public_ip"]
    region = get_region()
    profile = get_aws_profile()
    account_id = get_account_id()
    registry = ecr_registry(account_id, region)

    # 1. ECR login locally
    step(1, total, "Logging in to ECR...")
    _ecr_login_local(region, profile, registry)
    ok(f"logged in to {registry}")

    # 2. Build + push klite
    step(2, total, "Building klite image (linux/arm64)...")
    klite_tag = f"{registry}/klite:latest"
    run_local([
        "docker", "buildx", "build", "--platform", "linux/arm64",
        "-t", klite_tag, "-f", "Dockerfile", "--push", ".",
    ], cwd=REPO_ROOT)
    ok("klite image pushed")

    # 3. Build + push bench
    step(3, total, "Building klite-bench image (linux/arm64)...")
    bench_tag = f"{registry}/klite-bench:latest"
    run_local([
        "docker", "buildx", "build", "--platform", "linux/arm64",
        "-t", bench_tag, "-f", "Dockerfile.bench", "--push", ".",
    ], cwd=REPO_ROOT)
    ok("klite-bench image pushed")

    # 4. Pull on instances
    step(4, total, "Pulling images on EC2 instances...")
    ecr_login_and_pull = (
        f"aws ecr get-login-password --region {region}"
        f" | docker login --username AWS --password-stdin {registry}"
    )
    ssh(klite_ip, f"{ecr_login_and_pull} && docker pull {klite_tag}")
    ok("klite image pulled on klite instance")
    ssh(bench_ip, f"{ecr_login_and_pull} && docker pull {bench_tag}")
    ok("klite-bench image pulled on bench instance")

    console.print("\n[green bold]Images updated on both instances.[/]")


@app.command(name="run")
def run_bench(
    mode: Annotated[str, typer.Option(help="Bench mode: produce-consume, produce, consume")] = "produce-consume",
    topic: Annotated[str, typer.Option(help="Topic name")] = "bench",
    partitions: Annotated[int, typer.Option(help="Partition count")] = 6,
    num_records: Annotated[int, typer.Option(help="Measured records to produce (warmup is extra)")] = 360_000_000,
    record_size: Annotated[int, typer.Option(help="Bytes per record")] = 1024,
    producers: Annotated[int, typer.Option(help="Producer count")] = 4,
    consumers: Annotated[int, typer.Option(help="Consumer count")] = 4,
    acks: Annotated[int, typer.Option(help="Required acks: -1, 0, 1")] = 1,
    throughput: Annotated[int, typer.Option(help="Records/sec cap (-1 = unlimited)")] = 100_000,
    warmup_records: Annotated[int, typer.Option(help="Warmup records (excluded from stats)")] = 50_000,
    reporting_interval: Annotated[int, typer.Option(help="Report interval in ms")] = 60_000,
    label: Annotated[str, typer.Option(help="Label appended to output filename")] = "",
) -> None:
    """Start klite, run a benchmark, poll until done, fetch results."""
    valid_modes = ("produce-consume", "produce", "consume")
    if mode not in valid_modes:
        die(f"Invalid mode '{mode}'. Must be one of: {', '.join(valid_modes)}")

    outputs = tf_output()
    klite_pub = outputs["klite_public_ip"]
    klite_priv = outputs["klite_private_ip"]
    bench_ip = outputs["bench_public_ip"]
    s3_bucket = outputs["s3_bucket"]
    ecr_klite = outputs["ecr_klite"]
    ecr_bench = outputs["ecr_klite_bench"]
    region = get_region()
    total = 8

    # Single run ID ties data dir, S3 prefix, and output file together
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # Print run config
    console.print(f"\n[bold]Benchmark run {run_id}:[/]")
    console.print(f"  mode={mode}  topic={topic}  partitions={partitions}")
    console.print(f"  records={num_records:,}  size={record_size}B  throughput={throughput:,} rec/s")
    console.print(f"  producers={producers}  consumers={consumers}  acks={acks}")
    console.print(f"  warmup={warmup_records:,}  interval={reporting_interval}ms")
    if label:
        console.print(f"  label={label}")
    console.print()

    # 1. Start klite with unique data dir and S3 prefix
    step(1, total, "Starting klite on broker instance...")
    klite_flags = (
        f"--data-dir /data"
        f" -advertised-addr {klite_priv}:9092"
        f" -s3-bucket {s3_bucket}"
        f" -s3-region {region}"
        f" -s3-prefix {run_id}"
    )
    for flag, val in KLITE_SERVER_FLAGS.items():
        klite_flags += f" -{flag} {val}"

    ssh(klite_pub, (
        "docker rm -f klite 2>/dev/null;"
        f" docker run -d --name klite --net host"
        f" -v /data/klite-{run_id}:/data"
        f" {ecr_klite}:latest"
        f" {klite_flags}"
    ))
    ok(f"klite started (data=/data/klite-{run_id}, s3-prefix={run_id})")

    # 2. Verify klite
    step(2, total, "Verifying klite is running...")
    time.sleep(3)
    r = ssh(klite_pub, "docker logs klite 2>&1 | tail -5", capture=True)
    console.print(f"  [dim]{r.stdout.strip()}[/]")

    r2 = ssh(klite_pub, "docker inspect --format='{{.State.Status}}' klite", capture=True)
    if r2.stdout.strip() != "running":
        ssh(klite_pub, "docker logs klite 2>&1 | tail -20")
        die("klite exited unexpectedly. Check logs above.")
    ok("klite is running")

    # 3. Create topic
    step(3, total, f"Creating topic '{topic}' with {partitions} partitions...")
    ssh(bench_ip, (
        f"docker run --rm --net host {ecr_bench}:latest"
        f" create-topic -bootstrap-server {klite_priv}:9092"
        f" -topic {topic} -partitions {partitions}"
    ))
    ok("topic created")

    # 4. Start benchmark
    step(4, total, f"Starting benchmark ({mode})...")
    bench_flags = (
        f"-bootstrap-server {klite_priv}:9092"
        f" -topic {topic}"
        f" -num-records {num_records}"
        f" -record-size {record_size}"
        f" -acks {acks}"
        f" -throughput {throughput}"
        f" -warmup-records {warmup_records}"
        f" -reporting-interval {reporting_interval}"
        f" -json-output /results/bench.jsonl"
    )
    if mode in ("produce-consume", "produce"):
        bench_flags += f" -producers {producers}"
        bench_flags += f" -batch-max-bytes 1048576 -linger-ms 5"
    if mode in ("produce-consume", "consume"):
        bench_flags += f" -consumers {consumers}"

    ssh(bench_ip, (
        "docker rm -f bench-run 2>/dev/null;"
        f" docker run -d --name bench-run --net host"
        f" -v /tmp/results:/results"
        f" {ecr_bench}:latest"
        f" {mode} {bench_flags}"
    ))
    ok("benchmark container started")

    # 5. Poll until done
    step(5, total, "Waiting for benchmark to finish (polling every 30s)...")
    console.print()
    poll_start = time.monotonic()

    while True:
        time.sleep(30)
        elapsed = time.monotonic() - poll_start
        elapsed_str = _format_duration(elapsed)

        # Check container state
        r = ssh(bench_ip, "docker inspect --format='{{.State.Status}}' bench-run 2>/dev/null || echo missing",
                capture=True)
        state = r.stdout.strip()

        if state == "running":
            r = ssh(bench_ip, "docker logs --tail 2 bench-run 2>&1", capture=True)
            last_line = r.stdout.strip().split("\n")[-1] if r.stdout.strip() else ""
            console.print(f"  [dim][{elapsed_str}][/] running — {last_line[:120]}")
        elif state == "exited":
            r = ssh(bench_ip, "docker inspect --format='{{.State.ExitCode}}' bench-run", capture=True)
            exit_code = r.stdout.strip()
            if exit_code == "0":
                console.print(f"  [green][{elapsed_str}] Benchmark finished successfully.[/]")
            else:
                console.print(f"  [red][{elapsed_str}] Benchmark exited with code {exit_code}.[/]")
                ssh(bench_ip, "docker logs --tail 20 bench-run 2>&1")
                die("Benchmark failed. Logs printed above.")
            break
        else:
            die(f"Unexpected container state: {state}")

    # 6. Fetch results
    step(6, total, "Downloading results...")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{run_id}-{mode}"
    if label:
        filename += f"-{label}"
    filename += ".jsonl"
    local_path = RESULTS_DIR / filename

    scp(bench_ip, "/tmp/results/bench.jsonl", local_path)
    ok(f"results saved to {local_path.relative_to(REPO_ROOT)}")

    # 7. Stop klite (graceful shutdown flushes WAL to S3)
    step(7, total, "Stopping klite (graceful shutdown flushes to S3)...")
    ssh(klite_pub, "docker stop -t 120 klite", check=False)
    ok("klite stopped")

    # 8. Verify S3 record count
    # Warmup records are produced on top of num_records, so total = both.
    expected_total = num_records + warmup_records
    step(8, total, "Verifying S3 record count...")
    s3_count = _verify_s3_count(bench_ip, ecr_bench, s3_bucket, region, run_id, expected_total)

    console.print(f"\n[green bold]Benchmark complete.[/]")
    console.print(f"  Results: [bold]{local_path.relative_to(REPO_ROOT)}[/]")
    if s3_count is not None:
        console.print(f"  S3 records: {s3_count:,} / {expected_total:,} expected")


@app.command()
def down(
    ssh_key: Annotated[str, typer.Option(
        help="SSH public key file or literal key. Default: ~/.ssh/id_ed25519.pub",
    )] = "",
) -> None:
    """Tear down all AWS infrastructure."""
    pubkey = get_ssh_pubkey(ssh_key)
    console.print("[cyan]Destroying infrastructure...[/]")
    if not TFVARS.exists():
        die(f"Missing {TFVARS}")
    run_local(
        ["terraform", "destroy", "-var-file=bench.tfvars",
         f"-var=ssh_public_key={pubkey}", "-auto-approve"],
        cwd=TF_DIR,
    )
    console.print("[green bold]Infrastructure destroyed.[/]")


@app.command()
def status() -> None:
    """Show benchmark and klite status on EC2 instances."""
    outputs = tf_output()
    klite_ip = outputs["klite_public_ip"]
    bench_ip = outputs["bench_public_ip"]

    console.print("[bold]klite instance:[/]")
    r = ssh(klite_ip, "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null || echo 'no containers'",
            capture=True, check=False)
    console.print(f"  {r.stdout.strip()}")
    r = ssh(klite_ip, "docker logs --tail 5 klite 2>&1", capture=True, check=False)
    if r.stdout.strip():
        console.print(f"  [dim]{r.stdout.strip()}[/]")

    console.print("\n[bold]bench instance:[/]")
    r = ssh(bench_ip, "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null || echo 'no containers'",
            capture=True, check=False)
    console.print(f"  {r.stdout.strip()}")
    r = ssh(bench_ip, "docker logs --tail 10 bench-run 2>&1", capture=True, check=False)
    if r.stdout.strip():
        console.print(f"  [dim]{r.stdout.strip()}[/]")


@app.command()
def ssh_klite() -> None:
    """Open an interactive SSH session to the klite instance."""
    outputs = tf_output()
    ip = outputs["klite_public_ip"]
    console.print(f"Connecting to klite ({ip})...")
    os.execvp("ssh", [*ssh_cmd(ip)])


@app.command()
def ssh_bench() -> None:
    """Open an interactive SSH session to the bench instance."""
    outputs = tf_output()
    ip = outputs["bench_public_ip"]
    console.print(f"Connecting to bench ({ip})...")
    os.execvp("ssh", [*ssh_cmd(ip)])


# ---------------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------------

def _verify_s3_count(
    bench_ip: str,
    ecr_bench: str,
    s3_bucket: str,
    region: str,
    run_id: str,
    expected_records: int,
) -> int | None:
    """Run s3-count on the bench instance and compare against expected records."""
    r = ssh(bench_ip, (
        f"docker run --rm --net host {ecr_bench}:latest"
        f" s3-count -bucket {s3_bucket} -prefix {run_id}/ -region {region} -json"
    ), capture=True, check=False)

    if r.returncode != 0:
        console.print(f"  [yellow]WARN[/] s3-count failed: {r.stdout.strip()}")
        return None

    try:
        data = json.loads(r.stdout.strip())
    except json.JSONDecodeError:
        console.print(f"  [yellow]WARN[/] s3-count returned invalid JSON: {r.stdout.strip()[:200]}")
        return None

    s3_records = data.get("records", 0)
    diff = s3_records - expected_records

    if diff == 0:
        ok(f"S3 record count matches: {s3_records:,}")
    elif diff > 0:
        console.print(f"  [yellow]WARN[/] S3 has {diff:,} more records than expected ({s3_records:,} vs {expected_records:,})")
    else:
        console.print(f"  [red bold]MISMATCH[/] S3 is missing {-diff:,} records ({s3_records:,} vs {expected_records:,})")

    # Print per-partition breakdown
    partitions = data.get("partitions", {})
    if partitions:
        console.print(f"  Per-partition: {', '.join(f'p{k}={v:,}' for k, v in sorted(partitions.items(), key=lambda x: int(x[0])))}")

    return s3_records


def _ecr_login_local(region: str, profile: str, registry: str) -> None:
    """Log in to ECR locally using docker login via pipe."""
    try:
        pw = subprocess.run(
            ["aws", "ecr", "get-login-password", "--region", region, "--profile", profile],
            check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as e:
        die(f"ECR get-login-password failed: {e.stderr}\n  Is your AWS profile '{profile}' configured?")
    try:
        subprocess.run(
            ["docker", "login", "--username", "AWS", "--password-stdin", registry],
            input=pw.stdout, check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as e:
        die(f"docker login failed: {e.stderr}")


def _format_duration(seconds: float) -> str:
    """Format seconds as 'Xh Ym Zs' or 'Ym Zs' or 'Zs'."""
    s = int(seconds)
    if s >= 3600:
        return f"{s // 3600}h {(s % 3600) // 60}m {s % 60}s"
    if s >= 60:
        return f"{s // 60}m {s % 60}s"
    return f"{s}s"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
