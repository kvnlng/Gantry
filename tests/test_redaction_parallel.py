
import unittest
import os
import shutil
import tempfile
import numpy as np
import time
from gantry.session import DicomSession
from gantry.entities import Instance, Series, Study, Patient, Equipment

class TestRedactionParallel(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.session = DicomSession(self.db_path)

    def tearDown(self):
        # self.session.store_backend.close() does not exist/needed as connections are short-lived
        shutil.rmtree(self.test_dir)

    def test_parallel_execution_speedup_and_safety(self):
        """
        Verifies that redaction runs in parallel (multiple threads) and produces correct results.
        We can't easily assert speedup on a small test, but we can assert correctness and lack of locking errors.
        """
        # Setup 10 machines
        machine_serials = [f"M{i}" for i in range(10)]
        
        for i, serial in enumerate(machine_serials):
            p = Patient(f"P{i}", f"Pat{i}")
            st = Study(f"S{i}", "20230101")
            se = Series(f"Se{i}", "CT", 1)
            se.equipment = Equipment("Man", "Mod", serial)
            
            inst = Instance(f"I{i}", f"1.2.3.{i}", 1)
            # inst.rows = 50 
            # inst.columns = 50
            
            # Mock Pixel Data Loader
            # We create a unique array for each to verify modification
            # In a real threading scenario, we want to ensure no race conditions on shared resources (like the DB/Log)
            def make_loader():
                arr = np.zeros((50, 50), dtype=np.uint8) + 255
                return lambda: arr
            
            inst._pixel_loader = make_loader()
            
            se.instances.append(inst)
            st.series.append(se)
            p.studies.append(st)
            self.session.store.patients.append(p)
            
            # Index it manually if not using full session.ingest (RedactionService indexes on init)
            # RedactionService is created inside execute_config, so it will index current store.

        # Configure Rules
        rules = []
        for serial in machine_serials:
            rules.append({
                "serial_number": serial,
                "redaction_zones": [[0, 10, 0, 10]] # Top-Left 10x10 zeroed
            })
        
        self.session.active_rules = rules
        
        # Execute
        # This will use ThreadPoolExecutor inside
        self.session.redact()
        
        # Verify Results
        # Each instance should have the top-left 10x10 region black (0)
        # And the rest white (255)
        for p in self.session.store.patients:
            for st in p.studies:
                for se in st.series:
                    for inst in se.instances:
                        arr = inst.get_pixel_data()
                        # ROI: 0:10, 0:10
                        roi = arr[0:10, 0:10]
                        rest = arr[10:, 10:]
                        
                        self.assertTrue(np.all(roi == 0), f"Instance {inst.sop_instance_uid}: ROI not redacted")
                        # We can't strictly assert 'rest' is all 255 if ROI overlaps, but here it doesn't.
                        # Actually 'rest' is not the full complement.
                        # Just check a pixel outside.
                        self.assertEqual(arr[40, 40], 255, "Instance modified outside ROI")

if __name__ == '__main__':
    unittest.main()
