
---

description: Run the 50GB Export Benchmark on Google Cloud
---

# High-Performance Export Stress Test (GCP)

This workflow guides you through running the large-scale (100k instances, ~50GB) stress test on a Google Cloud VM.

## 1. Provision VM

Use `gcloud` or the Console to create a VM.
**Recommendation**: `n2-standard-4` (4 vCPU, 16GB RAM) or higher.
**Disk**: At least 100GB SSD (`pd-ssd`) or Local SSD (`local-ssd`) for best IO performance.

```bash
gcloud compute instances create gantry-benchmark \
    --zone=us-central1-a \
    --machine-type=n2-standard-8 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=200GB \
    --boot-disk-type=pd-ssd
```

## 2. Setup Gantry

SSH into the machine:

```bash
gcloud compute ssh gantry-benchmark
```

Clone and Install (inside VM):

```bash
sudo apt-get update && sudo apt-get install -y git python3-pip python3-venv

git clone https://github.com/kevin/Gantry.git
cd Gantry

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
pip install psutil
```

## 3. Remote Execution (Recommended)

You can run the full benchmark from your local machine using the provided wrapper script. This will sync your local code to the VM and execute the selected scenario.

```bash
# Usage: ./tests/benchmarks/run_remote.sh [SCENARIO]
# Scenarios:
#   A - Baseline (50GB Single-Frame)
#   B - Mixed Load (50GB Single + Multi)
#   C - Scalability (100GB)

./tests/benchmarks/run_remote.sh C
```

## 4. Manual Execution

If you prefer to SSH in manually:

### Scenario A: Baseline (50GB Single-Frame)

*Standard stress test for high file I/O operations (100k files).*

```bash
# 1. Clean previous data
rm -rf data/* benchmark.db

# 2. Generate 100k instances (Single Frame)
python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 100000 \
    --patients 1000 \
    --frames 1

# 3. Run Benchmark
python3 tests/benchmarks/run_stress_test.py \
    --input data/benchmark_in \
    --output data/benchmark_out \
    --db benchmark.db
```

### Scenario B: Mixed Load (50GB Single + Multi)

*Tests memory stability with large objects (50MB/file).*

```bash
# 1. Clean previous data
rm -rf data/* benchmark.db

# 2. Generate Data (25GB Single + 25GB Multi)
python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 50000 \
    --patients 500 \
    --frames 1 \
    --prefix PATIENT_SINGLE

python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 500 \
    --patients 50 \
    --frames 100 \
    --prefix PATIENT_MULTI

# 3. Run Benchmark
python3 tests/benchmarks/run_stress_test.py \
    --input data/benchmark_in \
    --output data/benchmark_out \
    --db benchmark.db
```

### Scenario C: Scalability (100GB)

*Tests linear scaling by running 50GB then extending to 100GB.*

```bash
# 1. Start Clean
rm -rf data/* benchmark.db

# --- PHASE 1: 50GB ---
echo "--- Starting Phase 1 (50GB) ---"

# Generate Batch A
python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 50000 \
    --patients 500 \
    --frames 1 \
    --prefix PATIENT_SINGLE_A

python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 500 \
    --patients 50 \
    --frames 100 \
    --prefix PATIENT_MULTI_A

# Run Benchmark (50GB)
python3 tests/benchmarks/run_stress_test.py \
    --input data/benchmark_in \
    --output data/benchmark_out \
    --db benchmark.db

# --- PHASE 2: 100GB ---
echo "--- Starting Phase 2 (Extension to 100GB) ---"

# Generate Batch B (Appends to same directory)
python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 50000 \
    --patients 500 \
    --frames 1 \
    --prefix PATIENT_SINGLE_B

python3 tests/benchmarks/generate_dataset.py \
    --output data/benchmark_in \
    --count 500 \
    --patients 50 \
    --frames 100 \
    --prefix PATIENT_MULTI_B

# Run Benchmark (100GB) - verifies linear scaling
python3 tests/benchmarks/run_stress_test.py \
    --input data/benchmark_in \
    --output data/benchmark_out \
    --db benchmark.db
```

## 5. Teardown

Don't forget to delete the VM!

```bash
gcloud compute instances delete gantry-benchmark
```
