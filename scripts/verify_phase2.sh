#!/usr/bin/env bash
# Reproduces every Phase 2 number in README.md / LEARNING.md from ONE fresh run, so they are consistent.
# Steps: clean start -> run -> SIGKILL processor -> restart producer -> memory -> stop producer
#        -> accounting check -> full replay -> report.
# WARNING: deletes existing pipeline data (docker compose down -v). Takes about 4 minutes.
# Usage (from the project folder): ./scripts/verify_phase2.sh

set -euo pipefail
cd "$(dirname "$0")/.."

kafka_tool() {  # run a Kafka CLI tool inside the broker container
  local tool=$1; shift
  docker compose exec -T kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@" 2>/dev/null
}
sql() { docker compose exec -T postgres psql -U pipeline -d upi -tA -c "$1"; }
end_offsets() { kafka_tool kafka-get-offsets.sh --topic "$1" | awk -F: '{s+=$3} END{print s+0}'; }

# Sum one column of `kafka-consumer-groups.sh --describe` over the 3 partitions.
# Columns: GROUP TOPIC PARTITION CURRENT-OFFSET(4) LOG-END-OFFSET(5) LAG(6). Prints -1 if a
# partition has no committed offset yet.
group_col_sum() {
  kafka_tool kafka-consumer-groups.sh --describe --group processor \
    | awk -v c="$1" '$2=="transactions.raw" && $c ~ /^[0-9]+$/ {s+=$c; n++} END{print (n==3 ? s : -1)}'
}
wait_lag_zero() {
  for _ in $(seq 1 90); do
    [ "$(group_col_sum 6)" = "0" ] && return 0
    sleep 2
  done
  echo "ERROR: processor did not catch up within 3 minutes"; exit 1
}
processor_counters() {  # counters from the processor's most recent summary log line
  docker compose logs processor | grep 'consumed=' | tail -1 \
    | grep -oE '(consumed|inserted|quarantined|redeliveries)=[0-9]+' | paste -sd' ' -
}
check() {  # check <label> <actual> <expected>
  if [ "$2" = "$3" ]; then echo "PASS  $1: $2 = $3"; else echo "FAIL  $1: $2 != $3"; fi
}

echo "== 1. Clean start (fresh Kafka + Postgres), run for 60 s"
docker compose down -v >/dev/null 2>&1
docker compose up --build -d >/dev/null 2>&1
sleep 60

echo "== 2. SIGKILL the processor (no clean shutdown), restart it 5 s later"
docker compose kill processor >/dev/null 2>&1
killed_at=$(date +%s)
# Read the committed offsets AFTER the kill: a dead consumer can't commit any more, so any
# later increase must come from the restarted processor.
before=$(group_col_sum 4)
sleep 5
docker compose start processor >/dev/null 2>&1
for _ in $(seq 1 120); do  # wait until the restarted processor commits a new offset
  [ "$(group_col_sum 4)" -gt "$before" ] && break
  sleep 1
done
echo "seconds from SIGKILL until processing resumed: $(( $(date +%s) - killed_at ))"

echo "== 3. Restart the producer (a restart must not re-send old txn_ids)"
sleep 20
docker compose restart producer >/dev/null 2>&1
sleep 30

echo "== 4. Memory, all containers running"
docker stats --no-stream --format '{{.Name}}: {{.MemUsage}}' | sed 's/project1-upi-pipeline-//'

echo "== 5. Stop the producer, wait for the processor to catch up (lag 0)"
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
echo "transactions=$txns quarantine=$quar"
echo "quarantine by reason: $(sql "SELECT r || '=' || count(*) FROM (SELECT split_part(reason, ':', 1) AS r FROM quarantine) q GROUP BY r ORDER BY r" | paste -sd' ' -)"
echo "fraud-labelled transactions: $(sql "SELECT count(*) FILTER (WHERE is_fraud) || ' of ' || count(*) || ' (' || round(100.0 * count(*) FILTER (WHERE is_fraud) / count(*), 1) || '%)' FROM transactions")"

echo "== 6. Full replay: reset the group's offsets to the start and reprocess everything"
docker compose stop processor >/dev/null 2>&1  # graceful stop prints the final counters
# Counters of the processor run that started after the SIGKILL (step 2 until now).
echo "processor counters after the SIGKILL restart: $(processor_counters)"
clean_before=$(end_offsets transactions.clean)
kafka_tool kafka-consumer-groups.sh --group processor --topic transactions.raw \
  --reset-offsets --to-earliest --execute >/dev/null
docker compose start processor >/dev/null 2>&1
wait_lag_zero
docker compose stop processor >/dev/null 2>&1  # graceful stop prints the final counters
echo "processor counters for the replay: $(processor_counters)"
check "transactions after replay (unchanged)" "$(sql "SELECT count(*) FROM transactions")" "$txns"
check "quarantine after replay (unchanged)" "$(sql "SELECT count(*) FROM quarantine")" "$quar"
echo "transactions.clean messages: before replay=$clean_before after replay=$(end_offsets transactions.clean)"

docker compose down >/dev/null 2>&1
echo "== Done (stack stopped, data volume kept)"
