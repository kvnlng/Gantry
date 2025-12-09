import pytest
import json
from gantry.config_manager import (
    ConfigLoader,
    ConfigValidationError,
    ROIValidator,
    ConfigSchemaValidator,
)


class TestROIValidator:
    """Tests for ROIValidator."""

    def test_valid_roi(self):
        """Valid ROI should return no errors."""
        errors = ROIValidator.validate([10, 50, 10, 50])
        assert errors == []

    def test_valid_roi_with_floats(self):
        """ROI with float values is also valid."""
        errors = ROIValidator.validate([10.0, 50.5, 10.0, 50.5])
        assert errors == []

    def test_negative_row_start(self):
        """Negative row_start should be rejected."""
        errors = ROIValidator.validate([-10, 50, 10, 50])
        assert len(errors) == 1
        assert "row_start" in errors[0]
        assert "negative" in errors[0]

    def test_negative_col_start(self):
        """Negative col_start should be rejected."""
        errors = ROIValidator.validate([10, 50, -10, 50])
        assert len(errors) == 1
        assert "col_start" in errors[0]

    def test_row_end_less_than_row_start(self):
        """row_end < row_start should be rejected."""
        errors = ROIValidator.validate([50, 10, 10, 50])
        assert len(errors) == 1
        assert "row_end" in errors[0]
        assert "greater than" in errors[0]

    def test_col_end_less_than_col_start(self):
        """col_end < col_start should be rejected."""
        errors = ROIValidator.validate([10, 50, 50, 10])
        assert len(errors) == 1
        assert "col_end" in errors[0]
        assert "greater than" in errors[0]

    def test_row_end_equals_row_start(self):
        """row_end == row_start should be rejected (zero height)."""
        errors = ROIValidator.validate([10, 10, 10, 50])
        assert len(errors) == 1
        assert "row_end" in errors[0]

    def test_wrong_length_too_short(self):
        """ROI with less than 4 values should be rejected."""
        errors = ROIValidator.validate([10, 50, 10])
        assert len(errors) == 1
        assert "exactly 4 values" in errors[0]

    def test_wrong_length_too_long(self):
        """ROI with more than 4 values should be rejected."""
        errors = ROIValidator.validate([10, 50, 10, 50, 100])
        assert len(errors) == 1
        assert "exactly 4 values" in errors[0]

    def test_not_a_list(self):
        """ROI must be a list."""
        errors = ROIValidator.validate("not a list")
        assert len(errors) == 1
        assert "must be a list" in errors[0]

    def test_non_numeric_value(self):
        """ROI must contain only numbers."""
        errors = ROIValidator.validate([10, "fifty", 10, 50])
        assert len(errors) >= 1
        assert "must be a number" in errors[0]

    def test_multiple_errors(self):
        """All errors should be collected."""
        errors = ROIValidator.validate([-10, -5, -3, -1])
        assert len(errors) >= 4

    def test_context_in_error_message(self):
        """Context should be included in error messages."""
        errors = ROIValidator.validate([-10, 50, 10, 50], context="test.roi")
        assert errors[0].startswith("test.roi:")


class TestConfigSchemaValidator:
    """Tests for ConfigSchemaValidator."""

    def test_valid_config(self):
        """Valid configuration should return no errors."""
        data = {
            "machines": [
                {
                    "serial_number": "SN-001",
                    "redaction_zones": [{"roi": [10, 50, 10, 50]}]
                }
            ]
        }
        errors = ConfigSchemaValidator.validate(data)
        assert errors == []

    def test_missing_machines_key(self):
        """Missing machines key is not an error (warning only)."""
        data = {"version": "1.0"}
        errors = ConfigSchemaValidator.validate(data)
        assert errors == []

    def test_machines_not_a_list(self):
        """machines must be a list."""
        data = {"machines": "not a list"}
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "must be a list" in errors[0]

    def test_machine_not_a_dict(self):
        """Each machine must be an object."""
        data = {"machines": ["not a dict"]}
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "must be an object" in errors[0]

    def test_missing_serial_number(self):
        """serial_number is required."""
        data = {
            "machines": [
                {"redaction_zones": [{"roi": [10, 50, 10, 50]}]}
            ]
        }
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "serial_number" in errors[0]

    def test_invalid_serial_number_type(self):
        """serial_number must be a string."""
        data = {
            "machines": [
                {"serial_number": 12345}
            ]
        }
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "serial_number" in errors[0]
        assert "string" in errors[0]

    def test_redaction_zones_not_a_list(self):
        """redaction_zones must be a list."""
        data = {
            "machines": [
                {"serial_number": "SN-001", "redaction_zones": "not a list"}
            ]
        }
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "redaction_zones" in errors[0]

    def test_zone_missing_roi(self):
        """Zone must contain roi."""
        data = {
            "machines": [
                {"serial_number": "SN-001", "redaction_zones": [{}]}
            ]
        }
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "roi" in errors[0]

    def test_invalid_roi_in_zone(self):
        """Invalid ROI in zone should be rejected."""
        data = {
            "machines": [
                {
                    "serial_number": "SN-001",
                    "redaction_zones": [{"roi": [50, 10, 10, 50]}]
                }
            ]
        }
        errors = ConfigSchemaValidator.validate(data)
        assert len(errors) == 1
        assert "row_end" in errors[0]


class TestConfigLoader:
    """Tests for ConfigLoader."""

    def test_load_valid_config(self, tmp_path):
        """Loading valid configuration."""
        data = {
            "machines": [
                {
                    "serial_number": "SN-001",
                    "redaction_zones": [{"roi": [10, 50, 10, 50]}]
                }
            ]
        }
        config_file = tmp_path / "valid.json"
        config_file.write_text(json.dumps(data))

        result = ConfigLoader.load_rules(str(config_file))
        assert len(result) == 1
        assert result[0]["serial_number"] == "SN-001"

    def test_load_invalid_roi_raises_error(self, tmp_path):
        """Loading config with invalid ROI should raise exception."""
        data = {
            "machines": [
                {
                    "serial_number": "SN-001",
                    "redaction_zones": [{"roi": [-10, 50, 10, 50]}]
                }
            ]
        }
        config_file = tmp_path / "invalid.json"
        config_file.write_text(json.dumps(data))

        with pytest.raises(ConfigValidationError) as exc_info:
            ConfigLoader.load_rules(str(config_file))

        assert "negative" in str(exc_info.value)

    def test_load_without_validation(self, tmp_path):
        """Loading without validation skips invalid ROIs."""
        data = {
            "machines": [
                {
                    "serial_number": "SN-001",
                    "redaction_zones": [{"roi": [-10, 50, 10, 50]}]
                }
            ]
        }
        config_file = tmp_path / "invalid.json"
        config_file.write_text(json.dumps(data))

        result = ConfigLoader.load_rules(str(config_file), validate=False)
        assert len(result) == 1

    def test_file_not_found(self):
        """Non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load_rules("nonexistent.json")

    def test_invalid_json(self, tmp_path):
        """Invalid JSON should raise ValueError."""
        config_file = tmp_path / "invalid.json"
        config_file.write_text("not valid json {{{")

        with pytest.raises(ValueError) as exc_info:
            ConfigLoader.load_rules(str(config_file))

        assert "Invalid JSON format" in str(exc_info.value)

    def test_missing_machines_key_returns_empty(self, tmp_path):
        """Missing machines key returns empty list."""
        data = {"version": "1.0"}
        config_file = tmp_path / "no_machines.json"
        config_file.write_text(json.dumps(data))

        result = ConfigLoader.load_rules(str(config_file))
        assert result == []
