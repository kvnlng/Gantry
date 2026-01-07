
import os
import time
import argparse
import pandas as pd
import subprocess
import shutil

# Phases configuration: (Total Instances, Phase Label)
# We assume cumulative growth.
# User asked for "Increase size 3 fold" for Phase 3.
# Phase 1: 50,000 (Baseline)
# Phase 2: 100,000 (2x)
# Phase 3: 300,000 (6x?) or 150k (3x of baseline)?
# Let's interpret "increase size of test 3 fold" as adding a 3rd phase that is 3x larger than PREVIOUS? or Total?
# Let's do: 50k -> 100k -> 300k.

PHASES = [
    {"target_count": 50000, "label": "Phase 1 (50k)"},
    {"target_count": 100000, "label": "Phase 2 (100k)"},
    {"target_count": 300000, "label": "Phase 3 (300k)"}
]

DATA_DIR = "data/benchmark_in"
OUT_DIR = "data/benchmark_out"
DB_PATH = "benchmark.db"

def run_command(cmd):
    print(f"DTOO: {cmd}")
    subprocess.check_call(cmd, shell=True)

def main():
    # Clean start
    if os.path.exists(DATA_DIR): shutil.rmtree(DATA_DIR)
    if os.path.exists(OUT_DIR): shutil.rmtree(OUT_DIR)
    if os.path.exists(DB_PATH): os.remove(DB_PATH)
    if os.path.exists(DB_PATH + "-shm"): os.remove(DB_PATH + "-shm")
    if os.path.exists(DB_PATH + "-wal"): os.remove(DB_PATH + "-wal")

    results = []

    current_count = 0 
    
    from run_stress_test import run_benchmark

    for phase in PHASES:
        target = phase["target_count"]
        needed = target - current_count
        label = phase["label"]
        
        print(f"\n{'#'*60}")
        print(f"STARTING {label} (Target: {target}, Adding: {needed})")
        print(f"{'#'*60}")
        
        # 1. Generate Data (Incremental)
        # We assume generate_dataset adds files if we change prefix or just adds to dir.
        # generate_dataset creates new Series dirs.
        # We need distinct prefixes to avoid collisions or just trust UUIDs.
        # Let's use prefix based on phase.
        
        if needed > 0:
            prefix = f"P{target}"
            # We mix 50% GantryGen, 50% Others (via random logic in generator)
            run_command(f"python3 tests/benchmarks/generate_dataset.py --output {DATA_DIR} --count {needed} --patients {max(10, needed//100)} --frames 1 --prefix {prefix}")
            current_count = target

        # 2. Run Benchmark
        # We run the pipeline on the ACCUMULATED dataset.
        # We capture the metrics returned by run_benchmark.
        # Note: run_benchmark currently prints. We need to modify it to RETURN metrics.
        # For now, let's just time the wrapper execution of the function? 
        # No, better to import and call, getting return values.
        
        stats = run_benchmark(DATA_DIR, OUT_DIR, DB_PATH, return_stats=True)
        stats["Phase"] = label
        stats["Total Instances"] = target
        results.append(stats)
        
    # Final Report
    df = pd.DataFrame(results)
    
    print("\n" + "="*60)
    print("SCALABILITY SUITE RESULTS")
    print("="*60)
    print(df.to_string(index=False))
    
    # Optional: Save CSV
    df.to_csv("benchmark_results.csv", index=False)
    print("\nSaved to benchmark_results.csv")

if __name__ == "__main__":
    main()
