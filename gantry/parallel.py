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
    total: int = None,
    executor: Any = None
) -> List[R]:
    """
    Executes func(item) in parallel using ProcessPoolExecutor.
    Displays a tqdm progress bar unless show_progress=False.
    Supports generators (pass total=N for progress bar).
    If 'executor' is passed, it uses that instance instead of creating a new one.
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
    # get_logger().debug(f"run_parallel using {mode_name} (Shared: {executor is not None})")

    if executor is not None:
        # Use shared executor (Caller manages lifecycle)
        # Note: We must ensure the executor type matches the requested mode if possible,
        # but typically the shared executor determines the mode.
        # We ignore force_threads if shared executor is passed unless we want to enforce verify?
        # For simplicity: Use whatever executor is passed.
        iterator = executor.map(func, items, chunksize=chunksize)
        
        if show_progress:
            if total is None and hasattr(items, '__len__'):
                total = len(items)
            results = list(tqdm(iterator, total=total, desc=desc))
        else:
            results = list(iterator)

    else:
        # Create new executor (Context Manager)
        with ExecutorClass(max_workers=max_workers) as internal_executor:
            iterator = internal_executor.map(func, items, chunksize=chunksize)
            
            if show_progress:
                if total is None and hasattr(items, '__len__'):
                    total = len(items)
                results = list(tqdm(iterator, total=total, desc=desc))
            else:
                results = list(iterator)

    return results
