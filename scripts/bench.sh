#!/usr/bin/env bash
#
# bench.sh — Run producer + consumer perf tests against klite.
#
# Builds klite and klite-bench, starts the broker, runs benchmarks,
# saves results tagged by git SHA. No JVM or external tools required.
#
# Usage:
#   ./scripts/bench.sh                    # concurrent produce + consume (default)
#   ./scripts/bench.sh --mode produce     # produce only
#   ./scripts/bench.sh --mode consume     # consume only (topic must have data)
#   ./scripts/bench.sh --mode both        # concurrent produce + consume
#   ./scripts/bench.sh --records 5000000  # override record count
#   ./scripts/bench.sh --size 100         # 100-byte records
#   ./scripts/bench.sh --partitions 1     # single partition
#   ./scripts/bench.sh --duration 120     # run producer for ~120s (throttled)
#   ./scripts/bench.sh --acks 1           # acks=1 instead of acks=-1 (all)
#   ./scripts/bench.sh --producers 4      # 4 concurrent producer clients
#   ./scripts/bench.sh --consumers 4      # 4 concurrent consumer clients
#   ./scripts/bench.sh --warmup 10000     # discard first 10k records from stats
#   ./scripts/bench.sh --skip-build       # reuse existing binaries
#   ./scripts/bench.sh --external         # don't start klite, connect to existing broker
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Defaults ---
MODE="both"
RECORDS=1000000
RECORD_SIZE=1000
PARTITIONS=6
THROUGHPUT=-1      # unlimited; overridden if --duration is set
ACKS=-1            # all
BATCH_SIZE=1048576
LINGER_MS=5
BROKER_ADDR="127.0.0.1:9092"
TOPIC="bench"
PRODUCERS=1
CONSUMERS=1
WARMUP=0
SKIP_BUILD=false
EXTERNAL=false
DURATION=""

# --- Parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)        MODE="$2";        shift 2 ;;
    --records)     RECORDS="$2";     shift 2 ;;
    --size)        RECORD_SIZE="$2"; shift 2 ;;
    --partitions)  PARTITIONS="$2";  shift 2 ;;
    --throughput)  THROUGHPUT="$2";  shift 2 ;;
    --duration)    DURATION="$2";    shift 2 ;;
    --acks)        ACKS="$2";        shift 2 ;;
    --batch-size)  BATCH_SIZE="$2";  shift 2 ;;
    --linger-ms)   LINGER_MS="$2";   shift 2 ;;
    --broker)      BROKER_ADDR="$2"; shift 2 ;;
    --topic)       TOPIC="$2";       shift 2 ;;
    --producers)   PRODUCERS="$2";   shift 2 ;;
    --consumers)   CONSUMERS="$2";   shift 2 ;;
    --warmup)      WARMUP="$2";      shift 2 ;;
    --skip-build)  SKIP_BUILD=true;  shift ;;
    --external)    EXTERNAL=true;    shift ;;
    -h|--help)
      sed -n '3,/^$/s/^# \?//p' "$0"
      exit 0
      ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

case "$MODE" in
  produce|consume|both) ;;
  *) echo "Invalid mode: $MODE (must be produce, consume, or both)"; exit 1 ;;
esac

# If --duration is set, compute a capped throughput to spread RECORDS over that many seconds.
if [[ -n "$DURATION" && "$THROUGHPUT" == "-1" ]]; then
  THROUGHPUT=$(( RECORDS / DURATION ))
  echo "Throttling to ~${THROUGHPUT} records/sec to fill ${DURATION}s"
fi

BENCH="$ROOT_DIR/bin/klite-bench"

# --- Build ---
if [[ "$SKIP_BUILD" == false ]]; then
  echo "Building klite and klite-bench..."
  (cd "$ROOT_DIR" && go build -o bin/klite ./cmd/klite)
  (cd "$ROOT_DIR" && go build -o bin/klite-bench ./cmd/klite-bench)
fi

# --- Results directory ---
GIT_SHA=$(cd "$ROOT_DIR" && git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_DIRTY=$(cd "$ROOT_DIR" && git diff --quiet 2>/dev/null && echo "" || echo "-dirty")
TIMESTAMP=$(date +%Y%m%dT%H%M%S)
RESULTS_DIR="$ROOT_DIR/tmp/bench/${TIMESTAMP}-${GIT_SHA}${GIT_DIRTY}"
mkdir -p "$RESULTS_DIR"

# Save parameters
cat > "$RESULTS_DIR/params.json" <<EOF
{
  "timestamp": "$TIMESTAMP",
  "git_sha": "${GIT_SHA}${GIT_DIRTY}",
  "mode": "$MODE",
  "records": $RECORDS,
  "record_size": $RECORD_SIZE,
  "partitions": $PARTITIONS,
  "throughput": $THROUGHPUT,
  "acks": $ACKS,
  "batch_size": $BATCH_SIZE,
  "linger_ms": $LINGER_MS,
  "producers": $PRODUCERS,
  "consumers": $CONSUMERS,
  "warmup": $WARMUP,
  "broker": "$BROKER_ADDR",
  "topic": "$TOPIC"
}
EOF

# --- Start klite (unless --external) ---
KLITE_PID=""
cleanup() {
  if [[ -n "$KLITE_PID" ]]; then
    echo "Stopping klite (pid $KLITE_PID)..."
    kill "$KLITE_PID" 2>/dev/null || true
    wait "$KLITE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ "$EXTERNAL" == false ]]; then
  DATA_DIR=$(mktemp -d)
  echo "Starting klite on $BROKER_ADDR (data: $DATA_DIR)..."
  "$ROOT_DIR/bin/klite" \
    --listen "$BROKER_ADDR" \
    --data-dir "$DATA_DIR" \
    --auto-create-topics=true \
    --default-partitions "$PARTITIONS" \
    --log-level warn \
    &
  KLITE_PID=$!

  # Wait for broker to accept connections
  echo -n "Waiting for broker"
  for i in $(seq 1 30); do
    if nc -z 127.0.0.1 "${BROKER_ADDR##*:}" 2>/dev/null; then
      echo " ready"
      break
    fi
    if [[ $i -eq 30 ]]; then
      echo " TIMEOUT"
      exit 1
    fi
    echo -n "."
    sleep 0.2
  done
fi

# --- Common args ---
PRODUCE_ARGS=(
  --bootstrap-server "$BROKER_ADDR"
  --topic "$TOPIC"
  --num-records "$RECORDS"
  --record-size "$RECORD_SIZE"
  --throughput "$THROUGHPUT"
  --acks "$ACKS"
  --batch-max-bytes "$BATCH_SIZE"
  --linger-ms "$LINGER_MS"
  --producers "$PRODUCERS"
  --warmup-records "$WARMUP"
)

CONSUME_ARGS=(
  --bootstrap-server "$BROKER_ADDR"
  --topic "$TOPIC"
  --num-records "$RECORDS"
  --consumers "$CONSUMERS"
)

# --- Create topic ---
if [[ "$MODE" != "consume" ]]; then
  echo "Creating topic '$TOPIC' with $PARTITIONS partitions..."
  "$BENCH" create-topic \
    --bootstrap-server "$BROKER_ADDR" \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS"
fi

# ==============================
# Mode: produce only
# ==============================
if [[ "$MODE" == "produce" ]]; then
  echo ""
  echo "=== PRODUCE BENCHMARK ==="
  echo "Records: $RECORDS | Size: ${RECORD_SIZE}B | Acks: $ACKS | Producers: $PRODUCERS | Throughput cap: $THROUGHPUT"
  echo ""

  "$BENCH" produce "${PRODUCE_ARGS[@]}" \
    --json-output "$RESULTS_DIR/produce.json" \
    2>&1 | tee "$RESULTS_DIR/produce.txt"

# ==============================
# Mode: consume only
# ==============================
elif [[ "$MODE" == "consume" ]]; then
  echo ""
  echo "=== CONSUME BENCHMARK ==="
  echo "Consumers: $CONSUMERS"
  echo ""

  "$BENCH" consume "${CONSUME_ARGS[@]}" \
    --json-output "$RESULTS_DIR/consume.json" \
    2>&1 | tee "$RESULTS_DIR/consume.txt"

# ==============================
# Mode: both (concurrent)
# ==============================
elif [[ "$MODE" == "both" ]]; then
  echo ""
  echo "=== CONCURRENT PRODUCE + CONSUME BENCHMARK ==="
  echo "Records: $RECORDS | Size: ${RECORD_SIZE}B | Acks: $ACKS | Producers: $PRODUCERS | Consumers: $CONSUMERS | Throughput cap: $THROUGHPUT"
  echo ""

  # Start producer in background
  "$BENCH" produce "${PRODUCE_ARGS[@]}" \
    --json-output "$RESULTS_DIR/produce.json" \
    > "$RESULTS_DIR/produce.txt" 2>&1 &
  PRODUCER_PID=$!

  # Give the producer a moment to start writing so the consumer
  # doesn't immediately time out with zero records.
  sleep 1

  # Start consumer in foreground
  "$BENCH" consume "${CONSUME_ARGS[@]}" \
    --json-output "$RESULTS_DIR/consume.json" \
    2>&1 | tee "$RESULTS_DIR/consume.txt"

  # Wait for producer to finish and capture its exit code
  PRODUCER_EXIT=0
  wait $PRODUCER_PID || PRODUCER_EXIT=$?

  echo ""
  echo "--- Producer results ---"
  cat "$RESULTS_DIR/produce.txt"

  if [[ $PRODUCER_EXIT -ne 0 ]]; then
    echo "WARNING: producer exited with code $PRODUCER_EXIT"
  fi
fi

# --- Cleanup ---
if [[ "$MODE" != "consume" ]]; then
  echo ""
  echo "Deleting topic '$TOPIC'..."
  "$BENCH" delete-topic \
    --bootstrap-server "$BROKER_ADDR" \
    --topic "$TOPIC"
fi

echo ""
echo "=== DONE ==="
echo "Results saved to: $RESULTS_DIR"
echo ""
echo "Files:"
ls -la "$RESULTS_DIR/"
