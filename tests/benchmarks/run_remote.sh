#!/bin/bash
# Remote Benchmark Runner for Gantry
# Usage: ./tests/benchmarks/run_remote.sh [SCENARIO]
# Scenarios: A (Baseline), B (Mixed), C (Scalability)

set -e

VM_NAME="gantry-benchmark"
ZONE="us-central1-a"
REMOTE_DIR="~/Gantry"
SCENARIO=${1:-A}

echo "--- Gantry Remote Benchmark Runner ---"
echo "VM: $VM_NAME ($ZONE)"
echo "Scenario: $SCENARIO"
echo "-------------------------------------"

# 1. Sync Code (using rsync via gcloud)
echo "[1/3] Syncing local code to VM..."
gcloud compute scp --recurse --zone=$ZONE . $VM_NAME:$REMOTE_DIR \
    --exclude ".git" \
    --exclude "venv" \
    --exclude "data" \
    --exclude "__pycache__"

# 2. Define Command based on Scenario
CMD_PREAMBLE="source $REMOTE_DIR/venv/bin/activate && cd $REMOTE_DIR"

case $SCENARIO in
  A)
    echo "[2/3] Running Scenario A: Baseline (50GB Single-Frame)..."
    REMOTE_CMD="
      rm -rf data/* benchmark.db;
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 100000 --patients 1000 --frames 1;
      python3 tests/benchmarks/run_stress_test.py --input data/benchmark_in --output data/benchmark_out --db benchmark.db
    "
    ;;
  B)
    echo "[2/3] Running Scenario B: Mixed Load (50GB Single + Multi)..."
    REMOTE_CMD="
      rm -rf data/* benchmark.db;
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 50000 --patients 500 --frames 1 --prefix PATIENT_SINGLE;
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 500 --patients 50 --frames 100 --prefix PATIENT_MULTI;
      python3 tests/benchmarks/run_stress_test.py --input data/benchmark_in --output data/benchmark_out --db benchmark.db
    "
    ;;
  C)
    echo "[2/3] Running Scenario C: Scalability (100GB)..."
    REMOTE_CMD="
      rm -rf data/* benchmark.db;
      echo '--- Phase 1: 50GB ---';
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 50000 --patients 500 --frames 1 --prefix PATIENT_SINGLE_A;
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 500 --patients 50 --frames 100 --prefix PATIENT_MULTI_A;
      python3 tests/benchmarks/run_stress_test.py --input data/benchmark_in --output data/benchmark_out --db benchmark.db;
      echo '--- Phase 2: 100GB ---';
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 50000 --patients 500 --frames 1 --prefix PATIENT_SINGLE_B;
      python3 tests/benchmarks/generate_dataset.py --output data/benchmark_in --count 500 --patients 50 --frames 100 --prefix PATIENT_MULTI_B;
      python3 tests/benchmarks/run_stress_test.py --input data/benchmark_in --output data/benchmark_out --db benchmark.db
    "
    ;;
  *)
    echo "Error: Unknown scenario '$SCENARIO'. Use A, B, or C."
    exit 1
    ;;
esac

# 3. Execute Remote Command
echo "[3/3] Executing on VM..."
gcloud compute ssh --zone=$ZONE $VM_NAME --command "$CMD_PREAMBLE && $REMOTE_CMD"

echo "-------------------------------------"
echo "Benchmark Complete."
