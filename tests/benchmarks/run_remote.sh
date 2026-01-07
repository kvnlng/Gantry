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
if ! dpkg -s python3.13-nogil >/dev/null 2>&1; then
    echo '[Remote] Installing Python 3.13 (Free-Threaded)...'
    sudo apt-get update -qq
    sudo apt-get install -y -qq software-properties-common
    sudo add-apt-repository -y ppa:deadsnakes/ppa
    sudo apt-get update -qq
    # Install standard 3.13 and experimental nogil/free-threaded build
    # Note: Package name varies, usually python3.13-nogil in deadsnakes
    sudo apt-get install -y -qq python3.13 python3.13-venv python3.13-dev python3.13-nogil
fi

# Detect generic python or free-threaded binary
PY_BIN=\"python3.13\"
if command -v python3.13t &> /dev/null; then
    PY_BIN=\"python3.13t\"
    echo \"[Remote] Found Free-Threaded Python: \$PY_BIN\"
elif command -v python3.13-nogil &> /dev/null; then
    PY_BIN=\"python3.13-nogil\"
    echo \"[Remote] Found Free-Threaded Python: \$PY_BIN\"
fi

if [ ! -d 'venv' ]; then
    echo \"[Remote] Creating virtual environment with \$PY_BIN...\"
    \$PY_BIN -m venv venv
fi

source venv/bin/activate

echo '[Remote] Installing Python dependencies...'
# Upgrade pip to ensure support for 3.13
pip install -q --upgrade pip
pip install -q -r requirements.txt
pip install -q psutil
# Install Gantry in editable mode
pip install -q -e .

# --- Execution ---
echo '[Remote] Starting Scalability Benchmark Suite...'
# Use the venv python (which is now linked to 3.13t if selected)
python tests/benchmarks/benchmark_suite.py
"

# 3. Execute Remote Command
echo "[3/3] Executing on VM..."
gcloud compute ssh --zone=$ZONE $VM_NAME --command "$REMOTE_SCRIPT"

echo "-------------------------------------"
echo "Benchmark Complete."
