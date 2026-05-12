#!/usr/bin/env bash
# Lattice runtime observation helper.
#
# Polls Prometheus counters and `DIAG[lattice]` log lines on a running
# cluster to verify that the Phase 2.4 runtime is doing what it should
# in observation-only mode.
#
# Run on a host that can reach the LN Prometheus endpoint(s). Defaults
# match the testnet topology (LN1 on :9091, LN2..LN10 on :9092..9100).
#
# Usage:
#   ./observe_lattice.sh                  # default 5min sample, all LNs
#   ./observe_lattice.sh 30               # 30s sample
#   ./observe_lattice.sh 300 9091         # 5min, single LN port

set -euo pipefail

DURATION_SECS="${1:-300}"
SINGLE_PORT="${2:-}"

# Default LN Prometheus ports for the testnet topology.
DEFAULT_PORTS=(9091 9092 9093 9094 9095 9096 9097 9098 9099 9100)
PORTS=("${SINGLE_PORT:-${DEFAULT_PORTS[@]}}")

echo "==================================================="
echo "Lattice runtime observation"
echo "Duration: ${DURATION_SECS}s"
echo "Ports:    ${PORTS[*]}"
echo "==================================================="

# Tab-separated header for easy spreadsheet import.
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
  "timestamp" "port" "cells_received" "cells_certified" \
  "cycles_committed" "pending_cells" "certified_cells"

END_AT=$(($(date +%s) + DURATION_SECS))

while [[ $(date +%s) -lt $END_AT ]]; do
  for port in "${PORTS[@]}"; do
    metrics=$(curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null || echo "")
    [[ -z "$metrics" ]] && continue

    cells_recv=$(echo "$metrics" | grep -E '^lattice_cells_received_total' | awk '{s+=$2} END {print s+0}')
    cells_cert=$(echo "$metrics" | grep -E '^lattice_cycles_committed_total' | awk '{s+=$2} END {print s+0}')
    cycles=$(echo "$metrics" | grep -E '^lattice_cycles_committed_total' | awk '{s+=$2} END {print s+0}')
    pending=$(echo "$metrics" | grep -E '^lattice_pending_cells' | awk '{s+=$2} END {print s+0}')
    certified=$(echo "$metrics" | grep -E '^lattice_certified_cells' | awk '{s+=$2} END {print s+0}')

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$(date -u +%H:%M:%S)" "$port" \
      "$cells_recv" "$cells_cert" "$cycles" "$pending" "$certified"
  done
  sleep 10
done

echo "==================================================="
echo "Observation finished. Healthy signals:"
echo "  - cells_received > 0   (gossip is flowing)"
echo "  - certified_cells > 0  (BFT attestation quorum reached)"
echo "  - cycles_committed > 0 (LineageCommit is producing decisions)"
echo "  - pending should stabilise, not grow unboundedly"
echo "==================================================="
