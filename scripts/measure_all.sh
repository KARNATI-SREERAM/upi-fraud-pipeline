#!/usr/bin/env bash
# Produces EVERY number in README.md and LEARNING.md from ONE fresh run, and saves the output to
# results/measured_run.txt. Steps: clean start -> model training -> live run -> SIGKILL recovery
# -> producer restart -> memory -> accounting checks -> live scoring quality -> API latency
# -> throughput benchmark -> full replay.
# WARNING: deletes existing pipeline data (docker compose down -v). Takes about 10-15 minutes.
# Needs the Mac venv: python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
# Usage (from the project folder): ./scripts/measure_all.sh

set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
exec > >(tee results/measured_run.txt) 2>&1  # everything below is printed AND saved

kafka_tool() {  # run a Kafka CLI tool inside the broker container
  local tool=$1; shift
  docker compose exec -T kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@" 2>/dev/null
}
sql() { docker compose exec -T postgres psql -U pipeline -d upi -tA -c "$1"; }
end_offsets() { kafka_tool kafka-get-offsets.sh --topic "$1" | awk -F: '{s+=$3} END{print s+0}'; }
# Sum one column of `kafka-consumer-groups.sh --describe` over the 3 partitions.
# Columns: GROUP TOPIC PARTITION CURRENT-OFFSET(4) LOG-END-OFFSET(5) LAG(6). -1 = not all committed.
group_col_sum() {
  kafka_tool kafka-consumer-groups.sh --describe --group processor \
    | awk -v c="$1" '$2=="transactions.raw" && $c ~ /^[0-9]+$/ {s+=$c; n++} END{print (n==3 ? s : -1)}'
}
wait_lag_zero() {
  for _ in $(seq 1 300); do
    [ "$(group_col_sum 6)" = "0" ] && return 0
    sleep 2
  done
  echo "ERROR: processor did not catch up within 10 minutes"; exit 1
}
processor_counters() {  # counters from the processor's most recent summary log line
  docker compose logs processor | grep 'consumed=' | tail -1 \
    | grep -oE '(consumed|inserted|flagged|quarantined|redeliveries)=[0-9]+' | paste -sd' ' -
}
row_total() { sql "SELECT (SELECT count(*) FROM transactions) + (SELECT count(*) FROM quarantine)"; }
check() {  # check <label> <actual> <expected>
  if [ "$2" = "$3" ]; then echo "PASS  $1: $2 = $3"; else echo "FAIL  $1: $2 != $3"; fi
}
# Precision / recall / F1 of the live `flagged` column against the simulator's labels.
quality() {  # quality <label> <SQL condition>
  sql "SELECT count(*) FILTER (WHERE flagged AND is_fraud), count(*) FILTER (WHERE flagged AND NOT is_fraud),
              count(*) FILTER (WHERE NOT flagged AND is_fraud), count(*) FILTER (WHERE is_fraud), count(*)
       FROM transactions WHERE $2" \
  | awk -F'|' -v l="$1" '{p=($1+$2)?$1/($1+$2):0; r=($1+$3)?$1/($1+$3):0; f=(p+r)?2*p*r/(p+r):0;
      printf "%s: txns=%d labelled_fraud=%d (%.2f%%) tp=%d fp=%d fn=%d precision=%.4f recall=%.4f f1=%.4f\n",
      l, $5, $4, 100*$4/$5, $1, $2, $3, p, r, f}'
}

echo "== 0. Environment"
# `git status --porcelain` also sees new untracked and staged files (git diff --quiet does not).
echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)  git: $(git rev-parse --short HEAD)$([ -z "$(git status --porcelain)" ] || echo ' (plus uncommitted changes)')"
echo "machine: $(sysctl -n machdep.cpu.brand_string), $(( $(sysctl -n hw.memsize) / 1073741824 )) GB RAM, macOS $(sw_vers -productVersion)"
echo "docker: $(docker version --format '{{.Server.Version}}'), $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}}' | awk '{printf "%s %s %.1f GiB available to containers", $1, $2, $3/1073741824}')"

echo "== 1. Clean start (fresh Kafka, Postgres and model)"
docker compose down -v >/dev/null 2>&1
docker compose up --build -d >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(docker compose ps api --format '{{.Health}}')" = "healthy" ] && break; sleep 2; done
echo "containers: $(docker compose ps -a --format '{{.Service}}={{.State}}' | paste -sd' ' -)"

echo "== 2. Model training report (trainer container, held-out test set)"
docker compose logs --no-log-prefix trainer

echo "== 3. Live run: 90 s at the configured 20 txn/s"
sleep 90
echo "transactions.raw offsets per partition: $(kafka_tool kafka-get-offsets.sh --topic transactions.raw | paste -sd' ' -)"

echo "== 4. SIGKILL the processor, restart it 5 s later"
docker compose kill processor >/dev/null 2>&1
killed_at=$(date +%s)
before=$(group_col_sum 4)  # read AFTER the kill: a dead consumer can't commit any more
sleep 5
docker compose start processor >/dev/null 2>&1
for _ in $(seq 1 120); do
  [ "$(group_col_sum 4)" -gt "$before" ] && break
  sleep 1
done
echo "seconds from SIGKILL until processing resumed: $(( $(date +%s) - killed_at ))"

echo "== 5. Restart the producer (a restart must not re-send old txn_ids)"
sleep 20
docker compose restart producer >/dev/null 2>&1
sleep 30

echo "== 6. Memory, all long-running containers"
docker stats --no-stream --format '{{.Name}}: {{.MemUsage}}' | sed 's/project1-upi-pipeline-//'

echo "== 7. Stop the producer, wait for lag 0, check accounting"
docker compose stop producer >/dev/null 2>&1
wait_lag_zero
raw=$(end_offsets transactions.raw)
txns=$(sql "SELECT count(*) FROM transactions")
quar=$(sql "SELECT count(*) FROM quarantine")
finals=$(docker compose logs producer | grep 'final:')  # one line per producer run
sent=$(echo "$finals" | sed -E 's/.*sent=([0-9]+).*/\1/' | awk '{s+=$1} END{print s}')
dirty=$(echo "$finals" | sed -E 's/.*dirty_events=([0-9]+).*/\1/' | awk '{s+=$1} END{print s}')
echo "producer runs: $(echo "$finals" | wc -l | tr -d ' '), last reported rate: $(docker compose logs producer | grep -o 'rate=[0-9.]* txn/s' | tail -1)"
check "producer sent == messages in transactions.raw" "$sent" "$raw"
check "raw messages == transactions + quarantine" "$raw" "$((txns + quar))"
check "dirty events injected == records quarantined" "$dirty" "$quar"
check "duplicate txn_ids in transactions table" "$(sql "SELECT count(*) - count(DISTINCT txn_id) FROM transactions")" "0"
check "transactions without a fraud score" "$(sql "SELECT count(*) FROM transactions WHERE fraud_score IS NULL")" "0"
echo "transactions=$txns quarantine=$quar"
echo "quarantine by reason: $(sql "SELECT r || '=' || count(*) FROM (SELECT split_part(reason, ':', 1) AS r FROM quarantine) q GROUP BY r ORDER BY r" | paste -sd' ' -)"
quality "live scoring (simulator labels vs flagged)" "true"
echo "transactions.flagged messages: $(end_offsets transactions.flagged)  flagged rows: $(sql "SELECT count(*) FROM transactions WHERE flagged")"

echo "== 8. API"
echo "GET /health: $(curl -s localhost:8000/health)"
echo "GET /stats: $(curl -s localhost:8000/stats)"
.venv/bin/python benchmark/api_latency.py

echo "== 9. Throughput: 20,000-message backlog through the processor"
live_cutoff=$(sql "SELECT min(ts) FROM transactions")  # backlog event times are 6 h older than this
rows_before=$(row_total)
quar_before=$(sql "SELECT count(*) FROM quarantine")
tp_out=$(.venv/bin/python benchmark/throughput.py --n 20000)
echo "$tp_out"
backlog_sent=$(echo "$tp_out" | sed -nE 's/.*sent=([0-9]+).*/\1/p')
backlog_dirty=$(echo "$tp_out" | sed -nE 's/.*dirty_events=([0-9]+).*/\1/p')
check "rows after backlog == rows before + backlog messages" "$(row_total)" "$((rows_before + backlog_sent))"
check "backlog dirty events == new quarantine rows" "$(( $(sql "SELECT count(*) FROM quarantine") - quar_before ))" "$backlog_dirty"
quality "backlog scoring (simulator labels vs flagged)" "ts < '$live_cutoff'"

echo "== 10. Full replay: reset the group's offsets to the start and reprocess everything"
docker compose stop processor >/dev/null 2>&1  # graceful stop prints the final counters
echo "processor counters for the backlog run: $(processor_counters)"
total_txns=$(sql "SELECT count(*) FROM transactions"); total_quar=$(sql "SELECT count(*) FROM quarantine")
kafka_tool kafka-consumer-groups.sh --group processor --topic transactions.raw \
  --reset-offsets --to-earliest --execute >/dev/null
docker compose start processor >/dev/null 2>&1
wait_lag_zero
docker compose stop processor >/dev/null 2>&1
echo "processor counters for the replay: $(processor_counters)"
check "transactions after replay (unchanged)" "$(sql "SELECT count(*) FROM transactions")" "$total_txns"
check "quarantine after replay (unchanged)" "$(sql "SELECT count(*) FROM quarantine")" "$total_quar"

echo "== 11. Where the missed frauds are (live + backlog, all false negatives)"
echo "missed frauds: $(sql "WITH h AS (SELECT is_fraud, flagged, extract(epoch FROM ts - lag(ts) OVER (PARTITION BY payer_vpa ORDER BY ts)) AS secs FROM transactions) SELECT count(*) FILTER (WHERE secs IS NULL OR secs > 1) || ' first-in-burst (no txn by this payer in the previous 1 s), ' || count(*) FILTER (WHERE secs <= 1) || ' later-in-burst' FROM h WHERE is_fraud AND NOT flagged")"

docker compose down >/dev/null 2>&1
echo "== Done (stack stopped, data volume kept). Output saved to results/measured_run.txt"
