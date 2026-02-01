"""
Docstring for tests.benchmarks.benchmark_suite
"""
import os
import argparse
import subprocess
import shutil
import pandas as pd

# Phases configuration: (Total Instances, Phase Label)

PHASES = [
    {"target_count": 1, "label": "Phase 0 (1 Multi-Frame Files)"},
    {"target_count": 10, "label": "Phase 1 (10 Multi-Frame Files)"},
    {"target_count": 100, "label": "Phase 2 (100 Multi-Frame Files)"},
]

DATA_DIR = "data/benchmark_in"
OUT_DIR = "data/benchmark_out"
DB_PATH = "benchmark.db"

def run_command(cmd):
    """Run a suite of benchmarks to test scalability with multi-frame DICOM files."""

    subprocess.check_call(cmd, shell=True)

def main():
    """Main function to run the benchmark suite."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--reuse", action="store_true", help="Reuse cached data to save time (generates only missing files)")
    args = parser.parse_args()

    CACHE_DIR = "data/benchmark_cache"

    # Always clean output/db for correctness
    if os.path.exists(OUT_DIR): shutil.rmtree(OUT_DIR)
    if os.path.exists(DB_PATH): os.remove(DB_PATH)
    if os.path.exists(DB_PATH + "-shm"): os.remove(DB_PATH + "-shm")
    if os.path.exists(DB_PATH + "-wal"): os.remove(DB_PATH + "-wal")

    if args.reuse:
        print(f"REUSE MODE: Using cache at {CACHE_DIR}")
        os.makedirs(CACHE_DIR, exist_ok=True)
    else:
        print(f"FRESH MODE: Cleaning all data...")
        if os.path.exists(DATA_DIR): shutil.rmtree(DATA_DIR)
        if os.path.exists(CACHE_DIR): shutil.rmtree(CACHE_DIR)
        os.makedirs(CACHE_DIR, exist_ok=True)

    results = []

    from run_stress_test import run_benchmark

    for phase in PHASES:
        target = phase["target_count"]
        label = phase["label"]

        print(f"\n{'#'*60}")
        print(f"STARTING {label} (Target: {target})")
        print(f"{'#'*60}")

        # 1. Ensure Cache has enough files
        # Walk recursively to find all dcm files
        cache_files = []
        for root, dirs, files in os.walk(CACHE_DIR):
            for f in files:
                if f.endswith(".dcm"):
                    cache_files.append(os.path.join(root, f))

        current_cache_count = len(cache_files)

        needed_generation = max(0, target - current_cache_count)

        if needed_generation > 0:
            print(f"Generating {needed_generation} new files into cache...")
            prefix = f"P{target}"
            # Multi-Frame Range 100-1000 as requested. We generate into CACHE_DIR
            run_command(f"python3 tests/benchmarks/generate_dataset.py --output {CACHE_DIR} --count {needed_generation} --patients {max(1, needed_generation//2)} --frames '100-1000' --prefix {prefix} --compress")

            # Refresh cache list
            cache_files = []
            for root, dirs, files in os.walk(CACHE_DIR):
                for f in files:
                    if f.endswith(".dcm"):
                        cache_files.append(os.path.join(root, f))

        # 2. Populate Run Directory (DATA_DIR)
        # We want EXACTLY 'target' files in DATA_DIR
        if os.path.exists(DATA_DIR): shutil.rmtree(DATA_DIR)
        os.makedirs(DATA_DIR)

        # Select first N files
        cache_files.sort()
        selected_files = cache_files[:target]

        if len(selected_files) < target:
            # Check just in case generation logic (like the 5-file minimum) worked differently than expected
             print(f"Warning: requested {target}, but cache has {len(selected_files)}.")



             if len(selected_files) == 0:
                 raise RuntimeError(f"Cache only has {len(selected_files)}, expected {target} after generation!")

        print(f"Populating Run Directory with {len(selected_files)} files from Cache (Flattening)...")
        for src_path in selected_files:
            # Flatten: Use UUID or keep filename if unique.
            # Generating unique names to prevent collisions if flattened.
            # generate_dataset uses Instance_1.dcm in different folders.
            # So we MUST rename them.
            fname = os.path.basename(src_path)
            parent = os.path.basename(os.path.dirname(src_path))
            grandparent = os.path.basename(os.path.dirname(os.path.dirname(src_path)))

            # Name: Patient_Series_Instance.dcm
            new_name = f"{grandparent}_{parent}_{fname}"
            dst = os.path.join(DATA_DIR, new_name)

            try:
                # Try hardlink for speed
                os.link(src_path, dst)
            except OSError:
                shutil.copy2(src_path, dst)

        # 3. Run Benchmark
        stats = run_benchmark(DATA_DIR, OUT_DIR, DB_PATH, return_stats=True, compress_export=True)
        stats["Phase"] = label
        stats["Total Instances"] = target
        results.append(stats)

    # Final Report
    df = pd.DataFrame(results)
    df = df.round(2)

    print("\n" + "="*60)
    print("SCALABILITY SUITE RESULTS")
    print("="*60)
    print(df.to_string(index=False))

    # Optional: Save CSV
    df.to_csv("benchmark_results.csv", index=False)
    print("\nSaved to benchmark_results.csv")

if __name__ == "__main__":
    main()
