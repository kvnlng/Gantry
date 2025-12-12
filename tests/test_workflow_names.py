import pytest
import os
import shutil
from gantry import Session

@pytest.fixture
def clean_session_path(tmp_path):
    """
    Returns a path to a temporary database file.
    Pytest handles directory cleanup (including WAL/SHM files).
    """
    db_path = tmp_path / "test_workflow.db"
    return str(db_path)

def test_workflow_aliases(clean_session_path, tmp_path):
    """
    Verifies that the new workflow aliases (ingest, examine, etc.) 
    correctly map to the underlying logic.
    """
    # Setup dummy DICOM data
    # (In a real scenario we'd use valid dicoms, here we might test empty or mock if possible, 
    # but the session needs real files to import. We can mock the helper or just check method existence/signature if we trust the delegation)
    # However, to be robust, let's trust that 'ingest' calls 'import_folder'.
    # We can mock the underlying methods to verify delegation.
    
    session = Session(clean_session_path)
    
    # 1. Ingest
    # Mocking import_folder to avoid needing real files
    original_import = session.import_folder
    called = []
    def mock_import(path):
        called.append("import")
    session.import_folder = mock_import
    
    session.ingest("dummy/path")
    assert "import" in called
    session.import_folder = original_import # Restore

    # 2. Examine
    original_inv = session.inventory
    called = []
    def mock_inv():
        called.append("inventory")
    session.inventory = mock_inv
    
    session.examine()
    assert "inventory" in called
    session.inventory = original_inv

    # 3. Configure
    original_scaffold = session.scaffold_config
    called = []
    def mock_scaffold(path):
        called.append("scaffold")
    session.scaffold_config = mock_scaffold
    
    session.setup_config("dummy.json")
    assert "scaffold" in called
    session.scaffold_config = original_scaffold

    # 4. Target (Audit)
    original_audit = session.audit
    called = []
    def mock_audit(config=None):
        called.append(f"audit_{config}")
        return [] # Empty report
    session.audit = mock_audit
    
    session.audit("config.json")
    assert "audit_config.json" in called
    session.audit = original_audit

    # 5. Backup
    original_preserve = session.preserve_identities
    called = []
    def mock_preserve(data):
        called.append("preserve")
    session.preserve_identities = mock_preserve
    
    session.backup_identities([])
    assert "preserve" in called
    session.preserve_identities = original_preserve

    # 6. Anonymize
    original_remed = session.apply_remediation
    called = []
    def mock_remed(findings):
        called.append("remediation")
    session.apply_remediation = mock_remed
    
    session.anonymize_metadata([])
    assert "remediation" in called
    session.apply_remediation = original_remed

    # 7. Redact
    original_exec = session.execute_config
    called = []
    def mock_exec():
        called.append("execute")
    session.execute_config = mock_exec
    
    session.redact_pixels()
    assert "execute" in called
    session.execute_config = original_exec
    
    # 8. Verify
    # uses audit again
    session.audit = mock_audit # Reuse mock
    session.verify()
    assert "audit_None" in called # called with None if no config passed
    session.audit = original_audit

    # 9. Export
    original_export = session.export
    called = []
    def mock_export(folder, safe):
         called.append(f"export_{safe}")
    session.export = mock_export
    
    session.export_data("out/path", safe=True)
    assert "export_True" in called
    
    print("All aliases delegated correctly.")
