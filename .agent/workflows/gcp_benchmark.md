
---

description: Run the 50GB Export Benchmark on Google Cloud
---

# High-Performance Export Stress Test (GCP)

This workflow guides you through running the **Scalability Benchmark Suite** on a Google Cloud VM.
The suite performs a 3-Phase stress test using massive, multi-frame, compressed DICOM datasets to validate the Gantry pipeline's stability and performance at scale.

## 1. Provision VM

Use `gcloud` or the Console to create a VM.

* **Machine Type**: `n2-standard-4` (4 vCPU, 16GB RAM) or higher.
* **Boot Disk**: **2TB pd-ssd**.
  * *Why?* The benchmark generates ~412GB of raw data, persists ~412GB in the sidecar database, and exports ~200GB (compressed). Total peak usage exceeds 1TB.

```bash
gcloud compute instances create gantry-benchmark \
    --zone=us-central1-a \
    --machine-type=n2-standard-8 \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size=2TB \
    --boot-disk-type=pd-ssd
```

## 2. Setup Gantry

SSH into the machine:

```bash
gcloud compute ssh gantry-benchmark --zone=us-central1-a
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
pip install -e .
```

## 3. Remote Execution (Recommended)

You can run the full benchmark suite from your **local machine** using the provided wrapper script. This script acts as a controller: it syncs your local code to the remote VM (via Git) and triggers the suite.

```bash
# Execute the Scalability Suite (Phases 1-3)
./tests/benchmarks/run_remote.sh
```

**What happens?**
The suite executes 3 incremental phases:

1. **Phase 1 (500 Files)**: Generates ~135GB of Multi-Frame (100-1000 frames) Compressed (RLE) DICOMs.
    * Runs full privacy pipeline (Ingest -> Redact -> Export j2k).
2. **Phase 2 (1000 Files)**: Appends another 500 files (~270GB Total).
    * Re-runs pipeline on total dataset.
3. **Phase 3 (1500 Files)**: Appends final 500 files (~412GB Total).
    * Re-runs pipeline on total dataset.

## 4. Manual Execution (Debugging)

If you need to debug or run specific parts manually on the remote VM:

```bash
# Clean start
rm -rf data/* benchmark.db

# Run the Suite orchestrator directly
python3 tests/benchmarks/benchmark_suite.py
```

## 5. Teardown

**Important:** Delete the VM when finished to avoid storage costs for the 2TB disk.

```bash
gcloud compute instances delete gantry-benchmark --zone=us-central1-a
```
