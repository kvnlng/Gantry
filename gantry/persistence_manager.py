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
        self.running = False
        self.thread = None
        
        self._start_worker()
        
        atexit.register(self.shutdown)
        get_logger().info("PersistenceManager initialized.")

    def _start_worker(self):
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._worker, daemon=True)
            self.thread.start()
            get_logger().info("PersistenceManager worker thread started.")

    def flush(self):
        """Blocks until all tasks in the queue have been processed."""
        # Check for silent crash: Worker is dead, but queue has items.
        if (not self.thread or not self.thread.is_alive()) and not self.queue.empty():
            get_logger().warning("PersistenceManager worker was found dead/stopped with pending items during flush. Restarting to process backlog.")
            print("Restarting stopped Persistence Manager to process pending items...")
            self._start_worker()

        # Wait for queue to be empty, regardless of thread state
        # (If thread dies during join, we might hang? No, task_done() is called in finally block)
        # But if thread dies BEFORE get(), item remains.
        # Our modified start_worker handles restart.
        
        self.queue.join()

    def save_async(self, patients: List[Patient]):
        """
        Queues a save operation.
        Note: We pass the list reference. If the list is mutated immediately after, 
        there might be a race condition. 
        For robustness, we could shallow copy the list of patients: list(patients).
        Deep copying the entire graph is too expensive.
        """
        # Auto-restart if we were shut down
        if not self.running or not self.thread or not self.thread.is_alive():
            get_logger().info("PersistenceManager was stopped. Restarting worker for new save operation.")
            self._start_worker()

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
                    if self.running:
                         # Stale sentinel from previous shutdown - ignore it
                         self.queue.task_done()
                         continue
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
                    
            except queue.Empty:
                continue
            except Exception as e:
                get_logger().error(f"Worker crashed: {e}")

    def shutdown(self):
        """
        Stops the worker and waits for potential pending saves.
        """
        # Avoid double shutdown or shutdown if never started
        if not self.thread.is_alive():
            return

        get_logger().info("Shutting down PersistenceManager...")
        print("\nShutting down Gantry Persistence Manager...")
        
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
        print("Persistence Manager Stopped.")
