
"""
Headless Stress Test Runner for Gantry.
Executes Import -> Redact -> Export pipeline and reports metrics.
"""

import os
import time
import argparse
import resource
import shutil
import logging
from gantry.session import DicomSession
from gantry.logger import get_logger

def report_resource_usage(stage_name):
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Max RSS is in KB on Linux, bytes on Mac (usually, but let's assume KB/MB relevant)
    # Actually Python resource.getrusage behavior varies.
    # On Linux: KB. On Mac: Bytes.
    # We will just report raw and let user interpret or normalize if we detect OS.
    # Let's simple report MB (assuming KB input for Linux, dividing by 1024, or Mac dividing by 1024*1024?)
    # Safer to just print raw.
    print(f"[{stage_name}] Max RSS: {usage.ru_maxrss} (units OS dependent)")
    
def run_benchmark(input_dir, output_dir, db_path):
    print(f"--- Starting Safety Pipeline Stress Test ---")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print(f"DB: {db_path}")
    
    # ensure clean start
    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        
    start_global = time.time()
    
    # [1] Initialize & Ingest
    print("\n[Step 1 & 2] Ingest")
    t0 = time.time()
    sess = DicomSession(db_path)
    sess.ingest(input_dir)
    sess.save()
    duration_ingest = time.time() - t0
    print(f"Ingest Duration: {duration_ingest:.2f}s")
    
    # [3] Examine
    print("\n[Step 3] Examine")
    t0 = time.time()
    sess.examine()
    print(f"Examine Duration: {time.time() - t0:.2f}s")

    # [4] Configure (Create & Load)
    print("\n[Step 4] Configure")
    # Write a dynamic config compatible with generated data (Manufacturer="GantryGen")
    config_path = "stress_config.yaml"
    with open(config_path, "w") as f:
        f.write("""
privacy_profile: "basic"
date_jitter:
  min_days: -10
  max_days: -1
remove_private_tags: true
machines:
  - manufacturer: "GantryGen"
    serial_number: "*"
    redaction_zones:
      - [0, 10, 0, 10] # Tiny region to test ROI logic
""")
    sess.load_config(config_path)
    
    # [5] Audit (Measure Twice)
    print("\n[Step 5] Audit")
    t0 = time.time()
    report = sess.audit()
    print(f"Audit Found {len(report)} issues.")
    print(f"Audit Duration: {time.time() - t0:.2f}s")

    # [6] Backup (Reversibility)
    print("\n[Step 6] Backup Identity")
    t0 = time.time()
    sess.enable_reversible_anonymization()
    sess.lock_identities(report)
    sess.save()
    print(f"Backup Duration: {time.time() - t0:.2f}s")
    
    # [7] Anonymize (Metadata)
    print("\n[Step 7] Anonymize")
    t0 = time.time()
    sess.anonymize(report)
    print(f"Anonymize Duration: {time.time() - t0:.2f}s")
    
    # [8] Redact (Pixel Data)
    print("\n[Step 8] Redact")
    t0 = time.time()
    sess.redact()
    print(f"Redact Duration: {time.time() - t0:.2f}s")
    report_resource_usage("Post-Redact")
    
    # [9] Verify & Export (Cut Once)
    print("\n[Step 9] Export (Verify & Write)")
    t0 = time.time()
    sess.export(output_dir, safe=True)
    duration_export = time.time() - t0
    print(f"Export Duration: {duration_export:.2f}s")
    report_resource_usage("Post-Export")
    
    print(f"\n--- Pipeline Complete ---")
    print(f"Total Duration: {time.time() - start_global:.2f}s")
    print(f"Ingest: {duration_ingest:.2f}s")
    print(f"Export: {duration_export:.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--db", default="benchmark.db")
    args = parser.parse_args()
    
    run_benchmark(args.input, args.output, args.db)
