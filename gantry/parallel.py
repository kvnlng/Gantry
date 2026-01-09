import concurrent.futures
import os
import sys
import multiprocessing
from tqdm import tqdm
from typing import Callable, Iterable, List, Any, TypeVar

T = TypeVar('T')
R = TypeVar('R')

def run_parallel(
    func: Callable[[T], R],
    items: Iterable[T],
    desc: str = "Processing",
    max_workers: int = None,
    chunksize: int = 1,
    show_progress: bool = True,
    force_threads: bool = False,
    total: int = None,
    executor: Any = None,
    maxtasksperchild: int = None
) -> List[R]:
    """
    Executes func(item) in parallel.
    Uses ProcessPoolExecutor by default, or multiprocessing.Pool if maxtasksperchild is set.
    Displays a tqdm progress bar unless show_progress=False.
    Supports generators (pass total=N for progress bar).
    If 'executor' is passed, it uses that instance.
    """
    results = []
    
    # Use max_workers = os.cpu_count() * 1.5 by default
    # This provides better throughput for I/O and compression heavy tasks
    if max_workers is None:
        if os.environ.get("GANTRY_MAX_WORKERS"):
            try:
                max_workers = int(os.environ["GANTRY_MAX_WORKERS"])
            except ValueError:
                pass
        
        if max_workers is None:
            cpu_count = os.cpu_count() or 1
            max_workers = int(cpu_count * 1.5)

    # Determine Strategy
    # Priority: Env Var -> Free-Threading Detection -> Default (Process)
    use_threads = False
    
    if force_threads or os.environ.get("GANTRY_FORCE_THREADS") == "1":
        use_threads = True
    elif os.environ.get("GANTRY_FORCE_PROCESSES") == "1":
        use_threads = False
    else:
        # Detect Free-Threading (Python 3.13+)
        # sys._is_gil_enabled() returns False if GIL is disabled
        if hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled():
            use_threads = True
    

    ExecutorClass = concurrent.futures.ThreadPoolExecutor if use_threads else concurrent.futures.ProcessPoolExecutor
    mode_name = "Threads" if use_threads else "Processes"
    
    # Optional: Log strategy (debug level only)
    # from .logger import get_logger
    # get_logger().debug(f"run_parallel using {mode_name} (Shared: {executor is not None})")

    if executor is not None:
        # Use shared executor (Caller manages lifecycle)
        # Note: We must ensure the executor type matches the requested mode if possible,
        # but typically the shared executor determines the mode.
        # We ignore force_threads if shared executor is passed unless we want to enforce verify?
        # For simplicity: Use whatever executor is passed.
        
        # Check if it's a multiprocessing.Pool (has imap) or concurrent.futures.Executor (has map)
        if hasattr(executor, 'imap'):
            iterator = executor.imap(func, items, chunksize=chunksize)
        else:
            iterator = executor.map(func, items, chunksize=chunksize)
        
        if show_progress:
            if total is None and hasattr(items, '__len__'):
                total = len(items)
            results = list(tqdm(iterator, total=total, desc=desc))
        else:
            results = list(iterator)

    else:
        # Create new executor (Context Manager)
        
        # Special Case: If using Processes AND maxtasksperchild is requested,
        # we MUST use multiprocessing.Pool, because ProcessPoolExecutor doesn't support it.
        # We also explicit use 'spawn' context for safety on all platforms when recycling.
        if not use_threads and maxtasksperchild is not None:
            # Use multiprocessing.Pool with 'spawn' context
            ctx = multiprocessing.get_context("spawn")
            with ctx.Pool(processes=max_workers, maxtasksperchild=maxtasksperchild) as pool:
                iterator = pool.imap(func, items, chunksize=chunksize)
                
                # Manual iteration with timeout to catch hangs
                # tqdm iterating over a timeout-check loop
                
                # Create iterator
                it = pool.imap(func, items, chunksize=chunksize)
                
                pbar = None
                if show_progress:
                    if total is None and hasattr(items, '__len__'):
                        total = len(items)
                    pbar = tqdm(total=total, desc=desc)
                
                while True:
                    try:
                        # 60s timeout - if a single chunk takes longer than 60s, we suspect a hang
                        # Note: J2K is slow, but not 60s slow.
                        res = it.next(timeout=60)
                        results.append(res)
                        if pbar: pbar.update(1)
                    except StopIteration:
                        break
                    except multiprocessing.TimeoutError:
                        print("\n!! WORKER HANG DETECTED (Timeout 60s) !!")
                        # We cannot easily identify WHICH worker hung here without more complex logic,
                        # but we know the pool is stuck.
                        # Raising error to abort the run.
                        raise RuntimeError("Worker Pool Hung (Timeout)")
                
                if pbar: pbar.close()
        else:
            # Standard Executor (ThreadPool or ProcessPool)
            with ExecutorClass(max_workers=max_workers) as internal_executor:
                iterator = internal_executor.map(func, items, chunksize=chunksize)
                
                if show_progress:
                    if total is None and hasattr(items, '__len__'):
                        total = len(items)
                    results = list(tqdm(iterator, total=total, desc=desc))
                else:
                    results = list(iterator)

    return results
