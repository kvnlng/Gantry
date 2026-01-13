import concurrent.futures
import os
import sys
import multiprocessing
from tqdm import tqdm
from typing import Callable, Iterable, List, Any, TypeVar

T = TypeVar('T')
R = TypeVar('R')

def _gc_off():
    import gc
    gc.disable()

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
    maxtasksperchild: int = None,
    progress: bool = None,  # Alias for show_progress
    disable_gc: bool = False # Disable GC in worker processes
) -> List[R]:
    """
    Executes func(item) in parallel.
    Uses ProcessPoolExecutor by default, or multiprocessing.Pool if maxtasksperchild is set.
    Displays a tqdm progress bar unless show_progress=False.
    Supports generators (pass total=N for progress bar).
    If 'executor' is passed, it uses that instance.
    """
    
    # Alias handling
    if progress is not None:
        show_progress = progress
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
            # User requested 1:1 CPU mapping for stability/predictability
            max_workers = cpu_count

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
    
    # CRITICAL FIX: If maxtasksperchild is requested, we MUST use processes to support recycling.
    # This overrides the Free-Threading default preference for threads.
    if maxtasksperchild is not None:
        use_threads = False

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
            
            init = None
            if disable_gc:
                init = _gc_off

            with ctx.Pool(processes=max_workers, maxtasksperchild=maxtasksperchild, initializer=init) as pool:
                # Use imap_unordered to avoid head-of-line blocking hiding crashes
                # This helps debugging which specific item causes a hang.
                # Note: If order matters for the caller, we might need to resort. 
                # But parallel.py returns a list. If we build the list from unordered, it will be shuffled?
                # Yes. run_parallel contract implies list order matching input?
                # "func(item) in parallel... Returns List[R]" usually implies mapping order.
                # However, for debugging the hang, unordered is superior.
                # Let's try unordered, but if we need order, we must attach indices.
                # Given we are debugging a stress test export (where order doesn't matter for correctness, just results), 
                # we can use unordered for now or generally if we assume this is a generic tool.
                # Actually, Gantry generally expects checking results count.
                
                # Manual iteration with timeout to catch hangs
                # tqdm iterating over a timeout-check loop
                
                # Manual iteration with timeout to catch hangs
                # tqdm iterating over a timeout-check loop
                
                # Create iterator
                iterator = pool.imap_unordered(func, items, chunksize=chunksize)
                
                pbar = None
                if show_progress:
                    if total is None and hasattr(items, '__len__'):
                        total = len(items)
                    pbar = tqdm(total=total, desc=desc)
                
                while True:
                    try:
                        # 600s timeout (increased for heavy J2K compression on large multiframe)
                        res = iterator.next(timeout=600)
                        results.append(res)
                        if pbar:
                            pbar.update(1)
                    except StopIteration:
                        break
                    except multiprocessing.TimeoutError:
                        print("\n!! WORKER HANG DETECTED (Timeout 600s) !!")
                        # We cannot easily identify WHICH worker hung here without more complex logic,
                        # but we know the pool is stuck.
                        # Raising error to abort the run.
                        raise RuntimeError("Worker Pool Hung (Timeout)")
                
                if pbar: pbar.close()
        else:
            # Standard Executor (ThreadPool or ProcessPool)
            
            init = None
            if disable_gc and not use_threads:
                 init = _gc_off
            
            # ProcessPoolExecutor accepts initializer in 3.7+
            kwargs = {'max_workers': max_workers}
            if not use_threads and init:
                kwargs['initializer'] = init

            with ExecutorClass(**kwargs) as internal_executor:
                iterator = internal_executor.map(func, items, chunksize=chunksize)
                
                if show_progress:
                    if total is None and hasattr(items, '__len__'):
                        total = len(items)
                    results = list(tqdm(iterator, total=total, desc=desc))
                else:
                    results = list(iterator)


    return results
