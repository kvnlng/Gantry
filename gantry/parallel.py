import concurrent.futures
import os
import sys
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
    total: int = None
) -> List[R]:
    """
    Executes func(item) in parallel using ProcessPoolExecutor.
    Displays a tqdm progress bar unless show_progress=False.
    Supports generators (pass total=N for progress bar).
    """
    results = []
    
    # Use max_workers = os.cpu_count() * 1.5 by default
    # This provides better throughput for I/O and compression heavy tasks
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
    # get_logger().debug(f"run_parallel using {mode_name} (Free-Threaded: {use_threads})")

    with ExecutorClass(max_workers=max_workers) as executor:
        # submit all
        # We use map for simplicity, but as_completed allows earlier processing
        # However, map preserves order which might be nice (though not strictly required here)
        
        # Note: items should be picklable (if using processes) / thread-safe (if using threads)
        iterator = executor.map(func, items, chunksize=chunksize)
        
        if show_progress:
            # If total is unknown and items is generator, tqdm will show just stats without bar
            if total is None and hasattr(items, '__len__'):
                total = len(items)
            
            results = list(tqdm(iterator, total=total, desc=desc))
        else:
            results = list(iterator)

    return results
