import threading
import queue
import atexit
import time
from typing import List, Optional
from .entities import Patient
from .persistence import SqliteStore
from .logger import get_logger

class PersistenceManager:
    """
    Offloads persistence operations to a background thread to unblock the main thread.
    Ensures data consistency by queuing snapshots (shallow copies should suffice if objects aren't mutually mutated).
    Registers an atexit handler to ensure pending saves are written before exit.
    """
    def __init__(self, store_backend: SqliteStore):
        self.store_backend = store_backend
        self.queue = queue.Queue()
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()
        
        atexit.register(self.shutdown)
        get_logger().info("PersistenceManager started in background.")

    def flush(self):
        """Blocks until all tasks in the queue have been processed."""
        self.queue.join()

    def save_async(self, patients: List[Patient]):
        """
        Queues a save operation.
        Note: We pass the list reference. If the list is mutated immediately after, 
        there might be a race condition. 
        For robustness, we could shallow copy the list of patients: list(patients).
        Deep copying the entire graph is too expensive.
        """
        # Shallow copy the list itself so if the session adds/removes patients, we have the old list.
        # But if attributes of patients change, we see the change. This is usually acceptable "eventual consistency" for this UX.
        snapshot = list(patients)
        self.queue.put(snapshot)

    def _worker(self):
        while self.running:
            try:
                # Wait for work
                patients = self.queue.get(timeout=1.0) 
                
                # If we get a sentinel (None), we exit
                if patients is None:
                    self.queue.task_done()
                    break
                
                # Perform the save
                # We catch exceptions to prevent thread death
                try:
                    self.store_backend.save_all(patients)
                except Exception as e:
                    get_logger().error(f"Background save failed: {e}")
                finally:
                    self.queue.task_done()
                    
            except queue.Empty:
                continue
            except Exception as e:
                get_logger().error(f"Worker crashed: {e}")

    def shutdown(self):
        """
        Stops the worker and waits for potential pending saves.
        """
        if not self.thread.is_alive():
            return

        get_logger().info("Shutting down PersistenceManager...")
        
        # Determine if we have pending work
        pending = self.queue.qsize()
        if pending > 0:
            print(f"Waiting for {pending} pending save operations to complete...")
            get_logger().info(f"Waiting for {pending} pending save operations...")

        # Stop worker
        self.running = False
        # Wake up if sleeping on queue
        self.queue.put(None)
        
        self.thread.join(timeout=30)
        get_logger().info("PersistenceManager stopped.")
