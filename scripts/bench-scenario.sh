#!/usr/bin/env bash
#
# bench-scenario.sh — Run the standard 3-scenario benchmark suite.
#
# Runs three scenarios against a broker and saves structured JSON results:
#   1. Throughput   — max rate, 4 producers + 4 consumers, 500B records
#   2. Latency      — throttled 50K recs/sec, 4P+4C, 500B records
#   3. Fan-out      — max rate, 4 producers + 4 consumers, 1KB records, 12 partitions
#
# Usage:
#   ./scripts/bench-scenario.sh                                # run against local klite (1M records)
#   ./scripts/bench-scenario.sh --records 5000000              # longer run for more stable results
#   ./scripts/bench-scenario.sh --external --broker host:9092  # run against MSK or existing broker
#   ./scripts/bench-scenario.sh --skip-build                   # reuse existing binaries
#   ./scripts/bench-scenario.sh --label msk-3x-m5large         # tag results
#
# Results are saved to tmp/bench/<timestamp>-<sha>/scenario-{throughput,latency,fanout}/
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Defaults ---
BROKER_ADDR="127.0.0.1:9092"
RECORDS=1000000
WARMUP=10000
SKIP_BUILD=false
EXTERNAL=false
LABEL=""

# --- Parse args ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker)      BROKER_ADDR="$2"; shift 2 ;;
    --records)     RECORDS="$2";     shift 2 ;;
    --warmup)      WARMUP="$2";     shift 2 ;;
    --skip-build)  SKIP_BUILD=true;  shift ;;
    --external)    EXTERNAL=true;    shift ;;
    --label)       LABEL="$2";       shift 2 ;;
    -h|--help)
      sed -n '3,/^$/s/^# \?//p' "$0"
      exit 0
      ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

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
SUITE_DIR="$ROOT_DIR/tmp/bench/${TIMESTAMP}-${GIT_SHA}${GIT_DIRTY}"
if [[ -n "$LABEL" ]]; then
  SUITE_DIR="${SUITE_DIR}-${LABEL}"
fi
mkdir -p "$SUITE_DIR"

cat > "$SUITE_DIR/suite.json" <<EOF
{
  "timestamp": "$TIMESTAMP",
  "git_sha": "${GIT_SHA}${GIT_DIRTY}",
  "label": "$LABEL",
  "broker": "$BROKER_ADDR",
  "external": $EXTERNAL,
  "records_per_scenario": $RECORDS,
  "warmup_records": $WARMUP,
  "scenarios": ["throughput", "latency", "fanout"]
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
    --default-partitions 12 \
    --log-level warn \
    &
  KLITE_PID=$!

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

# run_scenario <name> <topic> <partitions> <record_size> <records> <throughput> <producers> <consumers> <warmup>
run_scenario() {
  local name="$1"
  local topic="$2"
  local partitions="$3"
  local record_size="$4"
  local records="$5"
  local throughput="$6"
  local producers="$7"
  local consumers="$8"
  local warmup="$9"

  local dir="$SUITE_DIR/$name"
  mkdir -p "$dir"

  echo ""
  echo "============================================"
  echo "  Scenario: $name"
  echo "  Topic: $topic | Partitions: $partitions"
  echo "  Records: $records | Size: ${record_size}B"
  echo "  Producers: $producers | Consumers: $consumers"
  echo "  Throughput cap: $throughput | Warmup: $warmup"
  echo "  Acks: all | Batch: 64KB | Linger: 5ms"
  echo "============================================"
  echo ""

  # Create topic
  "$BENCH" create-topic \
    --bootstrap-server "$BROKER_ADDR" \
    --topic "$topic" \
    --partitions "$partitions"

  cat > "$dir/params.json" <<PARAMS
{
  "scenario": "$name",
  "topic": "$topic",
  "partitions": $partitions,
  "record_size": $record_size,
  "records": $records,
  "throughput": $throughput,
  "producers": $producers,
  "consumers": $consumers,
  "warmup": $warmup,
  "acks": -1,
  "batch_max_bytes": 65536,
  "linger_ms": 5
}
PARAMS

  # Start producer in background
  "$BENCH" produce \
    --bootstrap-server "$BROKER_ADDR" \
    --topic "$topic" \
    --num-records "$records" \
    --record-size "$record_size" \
    --throughput "$throughput" \
    --acks -1 \
    --batch-max-bytes 65536 \
    --linger-ms 5 \
    --producers "$producers" \
    --warmup-records "$warmup" \
    --json-output "$dir/produce.json" \
    > "$dir/produce.txt" 2>&1 &
  local PRODUCER_PID=$!

  # Give producers a moment to start writing.
  sleep 2

  # Start consumer
  "$BENCH" consume \
    --bootstrap-server "$BROKER_ADDR" \
    --topic "$topic" \
    --num-records "$records" \
    --consumers "$consumers" \
    --timeout 60000 \
    --json-output "$dir/consume.json" \
    2>&1 | tee "$dir/consume.txt"

  # Wait for producer
  local PRODUCER_EXIT=0
  wait $PRODUCER_PID || PRODUCER_EXIT=$?

  echo ""
  echo "--- Producer results ($name) ---"
  cat "$dir/produce.txt"

  if [[ $PRODUCER_EXIT -ne 0 ]]; then
    echo "WARNING: producer exited with code $PRODUCER_EXIT"
  fi

  # Cleanup topic
  "$BENCH" delete-topic \
    --bootstrap-server "$BROKER_ADDR" \
    --topic "$topic"

  # Brief pause between scenarios to let things settle.
  sleep 2
}

echo ""
echo "=========================================="
echo "  klite-bench scenario suite"
echo "  Broker: $BROKER_ADDR"
echo "  Label: ${LABEL:-<none>}"
echo "  Records per scenario: $RECORDS"
echo "  Warmup: $WARMUP records"
echo "=========================================="

# ==============================
# Scenario 1: Throughput
# Max rate, 4P+4C, 500B records, 6 partitions
# ==============================
run_scenario "throughput" "bench-throughput" 6 500 "$RECORDS" -1 4 4 "$WARMUP"

# ==============================
# Scenario 2: Latency under load
# 50K recs/sec, 4P+4C, 500B records, 6 partitions
# ==============================
run_scenario "latency" "bench-latency" 6 500 "$RECORDS" 50000 4 4 "$WARMUP"

# ==============================
# Scenario 3: Fan-out
# Max rate, 4P+4C, 1KB records, 12 partitions
# ==============================
run_scenario "fanout" "bench-fanout" 12 1000 "$RECORDS" -1 4 4 "$WARMUP"

# --- Summary ---
echo ""
echo "=========================================="
echo "  ALL SCENARIOS COMPLETE"
echo "=========================================="
echo ""
echo "Results: $SUITE_DIR"
echo ""

echo "--- Summary ---"
printf "%-12s %12s %12s %12s %12s %12s\n" \
  "Scenario" "Produce MB/s" "Consume MB/s" "P50 ms" "P99 ms" "P99.9 ms"
printf "%-12s %12s %12s %12s %12s %12s\n" \
  "--------" "------------" "------------" "------" "------" "--------"

for scenario in throughput latency fanout; do
  dir="$SUITE_DIR/$scenario"
  if [[ -f "$dir/produce.json" && -f "$dir/consume.json" ]]; then
    p_mb=$(python3 -c "import json; d=json.load(open('$dir/produce.json')); print(f'{d[\"mb_per_sec\"]:.2f}')" 2>/dev/null || echo "N/A")
    c_mb=$(python3 -c "import json; d=json.load(open('$dir/consume.json')); print(f'{d[\"mb_per_sec\"]:.2f}')" 2>/dev/null || echo "N/A")
    p50=$(python3 -c "import json; d=json.load(open('$dir/produce.json')); print(d['p50_latency_ms'])" 2>/dev/null || echo "N/A")
    p99=$(python3 -c "import json; d=json.load(open('$dir/produce.json')); print(d['p99_latency_ms'])" 2>/dev/null || echo "N/A")
    p999=$(python3 -c "import json; d=json.load(open('$dir/produce.json')); print(d['p999_latency_ms'])" 2>/dev/null || echo "N/A")
    printf "%-12s %12s %12s %12s %12s %12s\n" "$scenario" "$p_mb" "$c_mb" "$p50" "$p99" "$p999"
  fi
done

echo ""
echo "Full results in: $SUITE_DIR"
