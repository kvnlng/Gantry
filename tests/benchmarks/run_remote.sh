#!/bin/bash
# Remote Benchmark Runner for Gantry
# Usage: ./tests/benchmarks/run_remote.sh [SCENARIO]
# Scenarios: A (Baseline), B (Mixed), C (Scalability)

set -e

VM_NAME="gantry-benchmark"
ZONE="us-central1-a"
REMOTE_DIR="Gantry"
SCENARIO=${1:-A}

echo "--- Gantry Remote Benchmark Runner ---"
echo "VM: $VM_NAME ($ZONE)"
echo "Scenario: $SCENARIO"
echo "-------------------------------------"

# 1. Define Remote Support Function
# We embed this script to run on the remote machine to handle setup + execution
REMOTE_SCRIPT="
set -e

# --- Git Sync ---
if [ ! -d \"$REMOTE_DIR\" ]; then
    echo '[Remote] Cloning repository...'
    git clone https://github.com/kvnlng/Gantry.git $REMOTE_DIR
fi

cd $REMOTE_DIR
echo '[Remote] Syncing with origin/main...'
git fetch origin
git reset --hard origin/main

# --- Bootstrap ---
echo '[Remote] Checking environment...'
if ! dpkg -s python3-venv >/dev/null 2>&1; then
    echo '[Remote] Installing system dependencies...'
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3-pip python3-venv
fi

if [ ! -d 'venv' ]; then
    echo '[Remote] Creating virtual environment...'
    python3 -m venv venv
fi

source venv/bin/activate

echo '[Remote] Installing Python dependencies...'
pip install -q -r requirements.txt
pip install -q psutil
pip install -q -e .

# --- Execution ---
echo '[Remote] Starting Scalability Benchmark Suite...'
# We ensure the suite script is executable or just run with python
python3 tests/benchmarks/benchmark_suite.py
"

# 3. Execute Remote Command
echo "[3/3] Executing on VM..."
gcloud compute ssh --zone=$ZONE $VM_NAME --command "$REMOTE_SCRIPT"

echo "-------------------------------------"
echo "Benchmark Complete."
