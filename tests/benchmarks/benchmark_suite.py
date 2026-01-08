
import os
import time
import argparse
import pandas as pd
import subprocess
import shutil

# Phases configuration: (Total Instances, Phase Label)

PHASES = [
    {"target_count": 5, "label": "Phase 1 (5 Multi-Frame Files)"},
    {"target_count": 100, "label": "Phase 2 (100 Multi-Frame Files)"},
    # {"target_count": 1000, "label": "Phase 3 (1000 Multi-Frame Files)"},
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
        
        if needed > 0:
            prefix = f"P{target}"
            # Multi-Frame Range 100-1000 as requested
            run_command(f"python3 tests/benchmarks/generate_dataset.py --output {DATA_DIR} --count {needed} --patients {max(1, needed//2)} --frames '100-1000' --prefix {prefix} --compress")
            current_count = target

        # 2. Run Benchmark
        # We run the pipeline on the ACCUMULATED dataset.
        # We capture the metrics returned by run_benchmark.
        # Note: run_benchmark currently prints. We need to modify it to RETURN metrics.
        # For now, let's just time the wrapper execution of the function? 
        # No, better to import and call, getting return values.
        
        stats = run_benchmark(DATA_DIR, OUT_DIR, DB_PATH, return_stats=True, compress_export=True)
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
