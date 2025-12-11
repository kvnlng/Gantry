import concurrent.futures
import os
from tqdm import tqdm
from typing import Callable, Iterable, List, Any, TypeVar

T = TypeVar('T')
R = TypeVar('R')

def run_parallel(
    func: Callable[[T], R],
    items: Iterable[T],
    desc: str = "Processing",
    max_workers: int = None,
    chunksize: int = 1
) -> List[R]:
    """
    Executes func(item) in parallel using ProcessPoolExecutor.
    Displays a tqdm progress bar.
    """
    results = []
    # If no items, return empty
    items_list = list(items)
    if not items_list:
        return []

    # Use max_workers = os.cpu_count() by default
    # If items are few, don't spawn too many
    if max_workers is None:
        max_workers = min(32, os.cpu_count() + 4) # Standard heuristic

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # submit all
        # We use map for simplicity, but as_completed allows earlier processing
        # However, map preserves order which might be nice (though not strictly required here)
        # Using list(tqdm(executor.map(...))) allows progress bar
        
        # Note: items should be picklable
        results = list(tqdm(executor.map(func, items_list, chunksize=chunksize), total=len(items_list), desc=desc, unit="item"))
        
    return results
