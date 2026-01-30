
import unittest
import os
import shutil
import tempfile
import sys
from gantry.session import DicomSession
# Ensure we can import the generator
sys.path.insert(0, os.path.abspath('.'))
import scripts.generate_redaction_example as gen

class TestDiscoveryIntegration(unittest.TestCase):

    def setUp(self):
        # Create temp environment
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test.db")
        self.session = DicomSession(self.db_path)
        
        # Override generator path to output to temp dir
        # The generator script is hardcoded to "test_data/redaction_examples"
        # We'll just patch where we ingest from, but we have to let it write where it wants
        # or monkeypatch it. 
        # Easier: Just run it, move files, or just ingest from the hardcoded path (if safe).
        # Actually, let's just use the logic from reproduce_issue_v2.py
        
        gen.main(output_dir=os.path.join(self.test_dir, "data")) # Generates data in the temp dir/data

    def tearDown(self):
        self.session.close()
        shutil.rmtree(self.test_dir)

    def test_proper_noun_merging(self):
        """
        Integration test verifying that 'Hospital' + Gap + 'PatientName' 
        are merged into a single PROPER_NOUN zone using asymmetric clustering.
        """
        # 1. Ingest
        self.session.ingest(os.path.join(self.test_dir, "data"))
        
        # 2. Identify Serial
        # We know the generator makes specific sets.
        # "SN-5506" was the problematic one (GE Revolution CT).
        # But generator is random seeded in main().
        # We should find ANY serial that has data.
        
        eqs = self.session.store.get_unique_equipment()
        target_serial = eqs[0].device_serial_number
        
        # 3. Discover Zones
        # Use low confidence to ensure we catch the faint text
        zones = self.session.discover_redaction_zones(
            target_serial, 
            sample_size=10, 
            min_confidence=50.0
        )
        
        # 4. Assertions
        found_proper_noun = False
        found_merged_zone = False
        
        for z in zones:
            z_type = z.get('type')
            z_rect = z.get('zone') # [y1, y2, x1, x2]
            width = z_rect[3] - z_rect[2]
            
            if z_type == "PROPER_NOUN":
                found_proper_noun = True
                
                # The "Hospital | Name" line is wide.
                # "Hospital" is ~100px. "Name" is ~150px. Gap is ~30px. Total ~280px.
                # If unmerged, max width is < 200.
                if width > 250:
                    found_merged_zone = True
        
        self.assertTrue(found_proper_noun, "Should detect at least one PROPER_NOUN zone")
        self.assertTrue(found_merged_zone, "Should detect a merged zone (Width > 250px) indicating Hospital and Name are combined")

if __name__ == '__main__':
    unittest.main()
