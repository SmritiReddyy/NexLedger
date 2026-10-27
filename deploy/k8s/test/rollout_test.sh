#!/bin/bash
# Runs the in-cluster load for 90 s and, 25 s in, disrupts the ledger service:
#   baseline   nothing
#   rollout    kubectl rollout restart (a normal deploy)
#   kill       force-delete one ledger pod (removed from the Service at once)
#   crash      SIGKILL the JVM in one ledger pod: it dies mid-request and the
#              container restarts, the closest thing to a real crash
# Prints the load result as JSON.
#
#   deploy/k8s/test/rollout_test.sh rollout [kube-context]
#
# LAG=1 also samples the projection consumers' Kafka lag every 2 s (to stderr).
# Each sample starts a JVM inside the Kafka pod, which slows a laptop-sized
# cluster noticeably, so leave it off when measuring throughput.
set -euo pipefail
MODE=${1:-baseline}
CTX=${2:-kind-nexledger}
k() { kubectl --context "$CTX" -n nexledger "$@"; }
cd "$(dirname "$0")"

k delete job rolling-load --ignore-not-found --wait=true >/dev/null
k create configmap rolling-load --from-file=rolling_load.py --dry-run=client -o yaml | k apply -f - >/dev/null
k apply -f load-job.yaml >/dev/null
k wait --for=condition=ready pod -l job-name=rolling-load --timeout=120s >/dev/null
sleep 25
LAGPID=""
if [ "${LAG:-0}" = 1 ]; then
LAGFILE=$(mktemp)
( start=$(date +%s)
  while true; do
    lag=$(k exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
            --describe --group ledger-read-side 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}')
    echo "$(( $(date +%s) - start )) $lag" >> "$LAGFILE"
    sleep 2
  done ) &
LAGPID=$!
trap 'kill $LAGPID 2>/dev/null; rm -f "$LAGFILE"' EXIT
fi
case "$MODE" in
  rollout) k rollout restart deploy/ledger-service >/dev/null
           k rollout status deploy/ledger-service --timeout=300s >/dev/null ;;
  kill)    victim=$(k get pod -l app=ledger-service -o name | head -1)
           k delete "$victim" --grace-period=0 --force >/dev/null 2>&1 ;;
  crash)   # The JVM is PID 1 in its container, and PID 1 ignores SIGKILL sent from
           # inside its own namespace, so kill it from the kind node.
           node=${CTX#kind-}-control-plane
           cid=$(docker exec "$node" crictl ps --name ledger-service -q | head -1)
           pid=$(docker exec "$node" crictl inspect --output go-template --template '{{.info.pid}}' "$cid")
           docker exec "$node" kill -9 "$pid" ;;
  baseline) ;;
  *) echo "mode must be baseline, rollout, kill or crash" >&2; exit 2 ;;
esac
k wait --for=condition=complete job/rolling-load --timeout=300s >/dev/null
if [ -n "$LAGPID" ]; then
  kill $LAGPID 2>/dev/null || true
  # Lag timeline: seconds since the disruption, total lag in messages.
  echo "consumer lag (t lag): $(tr '\n' ' ' < "$LAGFILE")" >&2
fi
k logs job/rolling-load | grep '^RESULT ' | sed 's/^RESULT //'
