
import pytest
import time
import os
from gantry.persistence_manager import PersistenceManager
from gantry.persistence import SqliteStore
from gantry.entities import Patient

class MockStore(SqliteStore):
    def __init__(self, db_path):
        self.db_path = db_path
        self.saved_patients = []
        
    def save_all(self, patients):
        # Flatten list for simplicity or just store the batch
        self.saved_patients.extend(patients)

@pytest.fixture
def pm(tmp_path):
    db_path = str(tmp_path / "test_pm.db")
    store = MockStore(db_path)
    pm = PersistenceManager(store)
    yield pm
    pm.shutdown()

def test_basic_lifecycle(pm):
    """Test start, save, flush."""
    assert pm.running
    assert pm.thread.is_alive()
    
    p = Patient("P1", "Test")
    pm.save_async([p])
    
    pm.flush()
    
    assert len(pm.store_backend.saved_patients) == 1
    assert pm.store_backend.saved_patients[0].patient_id == "P1"

def test_restart_behavior(pm):
    """Test that save_async restarts the worker if it was shut down."""
    p1 = Patient("P1", "Test1")
    pm.save_async([p1])
    pm.flush()
    assert len(pm.store_backend.saved_patients) == 1
    
    # Needs to be called explicitly to stop the thread
    pm.shutdown()
    assert not pm.running
    assert not pm.thread.is_alive()
    
    # Now try to save again - should trigger restart
    p2 = Patient("P2", "AutoRestart")
    pm.save_async([p2])
    
    # Flush should work (if restart worked)
    pm.flush()
    
    assert pm.running
    assert pm.thread.is_alive()
    assert len(pm.store_backend.saved_patients) == 2
    assert pm.store_backend.saved_patients[1].patient_id == "P2"

def test_stale_sentinel(pm):
    """
    Simulate a scenario where a stale 'None' (sentinel) is in the queue
    but the worker is supposed to be running.
    """
    # 1. Manually inject poison pill
    pm.queue.put(None)
    
    # 2. Add legitimate work behind it
    p1 = Patient("P1", "Survivor")
    pm.save_async([p1])
    
    # 3. Wait for flush - if the bug existed, worker would die on None and flush would hang/timeout
    # or P1 would never be processed.
    pm.flush()
    
    # Verify worker is still alive
    assert pm.running
    assert pm.thread.is_alive()
    
    # Verify P1 was processed
    assert len(pm.store_backend.saved_patients) == 1
    assert pm.store_backend.saved_patients[0].patient_id == "P1"
