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
    disable_gc: bool = False, # Disable GC in worker processes
    return_generator: bool = False # Implement streaming
) -> Any: # Union[List[R], Iterator[R]]
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

    # Internal Generator to handle 'yield' vs list construction
    def _execute():
        # Use max_workers = os.cpu_count() * 1.5 by default
        # This provides better throughput for I/O and compression heavy tasks
        nonlocal max_workers
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
        use_threads = False
        
        if force_threads or os.environ.get("GANTRY_FORCE_THREADS") == "1":
            use_threads = True
        elif os.environ.get("GANTRY_FORCE_PROCESSES") == "1":
            use_threads = False
        else:
            if hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled():
                use_threads = True
        
        if maxtasksperchild is not None:
            use_threads = False

        ExecutorClass = concurrent.futures.ThreadPoolExecutor if use_threads else concurrent.futures.ProcessPoolExecutor
        
        if executor is not None:
            # Use shared executor
            if hasattr(executor, 'imap'):
                iterator = executor.imap(func, items, chunksize=chunksize)
            else:
                iterator = executor.map(func, items, chunksize=chunksize)
            
            if show_progress:
                # Iterate and yield from tqdm
                iter_total = total
                if iter_total is None and hasattr(items, '__len__'):
                    iter_total = len(items)
                
                for res in tqdm(iterator, total=iter_total, desc=desc):
                    yield res
            else:
                yield from iterator

        else:
            # Create new executor (Context Manager)
            if not use_threads and maxtasksperchild is not None:
                # multiprocessing.Pool path
                ctx = multiprocessing.get_context("spawn")
                init = None
                if disable_gc:
                    init = _gc_off

                with ctx.Pool(processes=max_workers, maxtasksperchild=maxtasksperchild, initializer=init) as pool:
                    iterator = pool.imap_unordered(func, items, chunksize=chunksize)
                    
                    pbar = None
                    if show_progress:
                        iter_total = total
                        if iter_total is None and hasattr(items, '__len__'):
                            iter_total = len(items)
                        pbar = tqdm(total=iter_total, desc=desc)
                    
                    while True:
                        try:
                            res = iterator.next(timeout=600)
                            yield res
                            if pbar:
                                pbar.update(1)
                        except StopIteration:
                            break
                        except multiprocessing.TimeoutError:
                            print("\n!! WORKER HANG DETECTED (Timeout 600s) !!")
                            raise RuntimeError("Worker Pool Hung (Timeout)")
                    
                    if pbar: pbar.close()
            else:
                # Standard Executor
                init = None
                if disable_gc and not use_threads:
                     init = _gc_off
                
                kwargs = {'max_workers': max_workers}
                if not use_threads and init:
                    kwargs['initializer'] = init

                with ExecutorClass(**kwargs) as internal_executor:
                    iterator = internal_executor.map(func, items, chunksize=chunksize)
                    
                    if show_progress:
                        iter_total = total
                        if iter_total is None and hasattr(items, '__len__'):
                            iter_total = len(items)
                        for res in tqdm(iterator, total=iter_total, desc=desc):
                            yield res
                    else:
                        yield from iterator

    # Dispatch result type
    gen = _execute()
    if return_generator:
        return gen
    else:
        return list(gen)
