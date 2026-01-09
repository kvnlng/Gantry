
import unittest
import concurrent.futures
from unittest.mock import MagicMock, patch
from gantry.session import DicomSession
from gantry.io_handlers import DicomExporter

class TestExecutorRecovery(unittest.TestCase):
    
    @patch('gantry.session.DicomExporter.export_batch')
    @patch('gantry.session.DicomExporter.generate_export_from_db')
    @patch('gantry.session.concurrent.futures.ProcessPoolExecutor')
    def test_export_recovery(self, mock_executor_cls, mock_gen, mock_export_batch):
        """
        Verifies that DicomSession.export catches BrokenProcessPool and retries.
        """
        # Setup
        mock_export_batch.side_effect = [
            concurrent.futures.process.BrokenProcessPool("Mock Crash 1"),
            concurrent.futures.process.BrokenProcessPool("Mock Crash 2"),
            100 # Success count
        ]
        
        # Mock generator to return a list (so we don't care about consumed generators for this mock)
        mock_gen.return_value = ["ctx1", "ctx2"]
        
        # Initialize Session
        # We need to mock sqlitestore or avoid init logic that touches disk
        # DicomSession init touches disk and logger.
        
        # Let's mock SqliteStore too
        with patch('gantry.session.SqliteStore'), patch('gantry.session.PersistenceManager'), patch('os.path.exists', return_value=False):
            sess = DicomSession("dummy.db")
            
            # Setup initial executor mock
            mock_executor_instance_1 = MagicMock()
            mock_executor_instance_2 = MagicMock()
            mock_executor_instance_3 = MagicMock()
            
            def executor_side_effect(*args, **kwargs):
                if executor_side_effect.counter == 0:
                    executor_side_effect.counter += 1
                    return mock_executor_instance_1
                elif executor_side_effect.counter == 1:
                    executor_side_effect.counter += 1
                    return mock_executor_instance_2
                return mock_executor_instance_3
            
            executor_side_effect.counter = 0
            
            mock_executor_cls.side_effect = executor_side_effect
            
            # Assign initial executor to session (normally done in init)
            sess._executor = mock_executor_instance_1
            
            # Execute Export
            # Needs to patch valid patient IDs and Instance counts
            mock_p = MagicMock(patient_id="P1")
            mock_st = MagicMock()
            mock_se = MagicMock()
            # 1 instance
            mock_se.instances = [MagicMock()] 
            mock_st.series = [mock_se]
            mock_p.studies = [mock_st]
            
            sess.store.patients = [mock_p]
            
            # Act
            sess.export("dummy_out", safe=False)
            
            # Assertions
            

            
            print(f"DEBUG: Executor Mock Call Count: {mock_executor_cls.call_count}")
            # Verify Flow:
            # 1. Export Batch called THREE times (Double Retry happened)
            #    Call 1: Crash (Initial)
            #    Call 2: Crash (Safe Mode)
            #    Call 3: Success (Serial Mode)
            self.assertEqual(mock_export_batch.call_count, 3)
            
            # 2. Executor Constructor called (Restart happened TWICE)
            # Init + Restart 1 (Safe) + Restart 2 (Serial) = at least 3 calls
            self.assertGreaterEqual(mock_executor_cls.call_count, 3)
            
            # 3. Check Serial Fallback
            # The last call to constructor should have max_workers=1
            last_call_args = mock_executor_cls.call_args_list[-1]
            self.assertEqual(last_call_args[1].get('max_workers'), 1)
            
            print("\nTest Passed: Recovery logic successfully triggered and retried.")

if __name__ == "__main__":
    unittest.main()
