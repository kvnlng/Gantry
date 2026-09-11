"""
Configuration manager for handling Isocenter system settings.

This module provides functionality to load, validate, and manage configuration
files for the Isocenter application. It supports unified YAML configurations,
legacy formats, and privacy profile management.
"""

import os
import logging
import copy
from typing import Dict, Any, List, Optional
import re
import yaml

from dotenv import load_dotenv

from .profiles import FLOOR_POLICY, PRIVACY_PROFILES

CONFIG_VERSION = "2.0"

#: Where this package's own shipped resources live.
#:
#: Hoisted out of `load_phi_config`'s body in #388 so a test could
#: monkeypatch it. `load_phi_config` no longer reads a resource -- the
#: default PHI policy is `profiles.FLOOR_POLICY`, in Python, since #495
#: deleted `resources/phi_tags.json` -- and the constant stays because
#: `publish.yml`'s wheel gate passes it to `require_package_resource` from
#: an installed wheel, where it is the only spelling of "this package's
#: resources directory" that does not point back at the source tree.
RESOURCES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             "resources")

# Load environment variables
load_dotenv()


def require_package_resource(directory: str, basename: str,
                             consequence: str) -> str:
    """The path to a resource this package ships, or a refusal.

    Three loaders returned an empty collection when a shipped file was
    absent, with no log line and no audit row: the run then scanned every
    frame with no redaction rules, or audited against an empty PHI tag
    list, and reported clean (#388).

    **A refusal rather than a warning**, on #400's reasoning: a warning in
    front of a run that then succeeds is a line nobody reads, and there is
    nothing to *annotate*, because a missing shipped resource is never a
    correct state. The degrade-gracefully rule is about the optional
    extras (`ocr`, `nlp`, `docs`); a shipped package resource is the
    opposite kind of thing -- `setup.py`'s `package_data` promises it and
    `publish.yml` refuses to release a wheel without it, so this is the
    runtime half of a promise CI already makes. No audit row is written
    either, for the same reason.

    **`RuntimeError`, and deliberately not `FileNotFoundError`.**
    `ConfigLoader._load_yaml` already raises that for a *user's* config
    file, which is a different failure with a different remedy, and a
    caller writing `except FileNotFoundError` around `load_config` would
    silently swallow "your install is broken". The sharper reason is that
    the callers' own handlers are `except (OSError, ...)` and
    `FileNotFoundError` **is** an `OSError`: a refusal of that type, if it
    ever drifted inside one of those `try` blocks, would be caught and
    turned straight back into the empty collection this function exists to
    replace. Call it **before** the `try`.

    `directory` is a parameter rather than a module global read in here.
    Both callers' `RESOURCES_DIR` is what tests monkeypatch, and a helper
    that closed over its own copy would make every such test pass against
    the real source tree.

    `consequence` is the caller's own words for what continuing would have
    done. A generic sentence would be the same failure as a generic loss
    row: accurate, not generic, is the standard this applies to refusals
    as much as to anything else.

    Args:
        directory (str): the resources directory to look in.
        basename (str): the file's name, passed as a bare literal by every
            caller so `test_every_shipped_resource_is_named_by_the_package`
            can still see it in the AST.
        consequence (str): what a silent continue would have done, e.g.
            "scanned every frame with no machine redaction rules".

    Returns:
        str: the resolved path, which exists.

    Raises:
        RuntimeError: if the resource is not there.
    """
    path = os.path.join(directory, basename)
    if not os.path.exists(path):
        raise RuntimeError(
            f"Isocenter's shipped resource {basename} is missing from this "
            f"installation (looked in {path}). setup.py packages it and "
            f"publish.yml refuses to release a wheel without it, so its "
            f"absence is a broken install rather than a configuration "
            f"choice -- reinstall isocenter. Continuing would have "
            f"{consequence}, and reported a clean run.")
    return path


def get_logger() -> logging.Logger:
    """
    Retrieves the configured logger for the Isocenter application.

    Returns:
        logging.Logger: The 'isocenter' logger instance.
    """
    return logging.getLogger("isocenter")


def _lowercase_tag_keys(tags: Dict[Any, Any]) -> Dict[Any, Any]:
    """`tags` with every string key lowercased, order kept."""
    return {(key.lower() if isinstance(key, str) else key): value
            for key, value in tags.items()}


def _names_no_profile(profile_name: Any) -> bool:
    """True for `privacy_profile: none` and `privacy_profile: null`."""
    return profile_name is None or (
        isinstance(profile_name, str) and profile_name.strip().lower() == "none")


def load_unified_config(path: str) -> Dict[str, Any]:
    """
    Loads the unified configuration file (YAML).

    Supports legacy list-based config (machine rules only) and new dict-based config.
    Merges 'privacy_profile' if specified (Built-in or External).

    Args:
        path (str): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: The loaded configuration dictionary.

    Raises:
        ValueError: If file is not YAML.
    """
    if not (path.endswith('.yaml') or path.endswith('.yml')):
        raise ValueError("Configuration file must be a YAML file (.yaml or .yml)")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Handle Standard Config
    config = data

    # Lowercase the user's tag keys before anything is merged. The
    # profiles' keys are lowercase (profiles.py's header comment), so a
    # user's `0008,103E` merged as spelled sat beside the profile's
    # `0008,103e` as a second rule for one tag: `PhiInspector` collapsed
    # the pair at scan time with the later entry winning by dict order,
    # and the report counted both (#495).
    if isinstance(config.get("phi_tags"), dict):
        config["phi_tags"] = _lowercase_tag_keys(config["phi_tags"])

    # `privacy_profile: none` (or `null`): the file's `phi_tags` are the
    # whole policy, with no base beneath them. The scaffold's header has
    # told users to write this "for manual control" since v2.0, and it
    # warned "Unknown privacy profile" and loaded nothing until #495.
    if "privacy_profile" in config and _names_no_profile(config["privacy_profile"]):
        config.pop("privacy_profile")
        return config

    # Merge Privacy Profile
    if "privacy_profile" in config:
        profile_name = config["privacy_profile"]

        profile_rules = {}

        # 1. Check Built-in Profiles
        if profile_name in PRIVACY_PROFILES:
            profile_rules = copy.deepcopy(PRIVACY_PROFILES[profile_name])
            get_logger().info("Loaded built-in privacy profile '%s' with %d rules.", profile_name, len(profile_rules))

        # 2. Check External File (Custom Profile)
        elif os.path.exists(profile_name):
            try:
                # We reuse load_phi_config logic to parse just the tags
                profile_rules = ConfigLoader.load_phi_config(profile_name)
                get_logger().info("Loaded custom privacy profile from '%s' with %d rules.", profile_name, len(profile_rules))
            except (ValueError, OSError) as e:
                get_logger().error("Failed to load custom profile '%s': %s", profile_name, e)

        else:
            get_logger().warning("Unknown privacy profile reference '%s' (not a built-in or file). Ignoring.", profile_name)

        if not profile_rules:
            # Ignoring it means ignoring it everywhere. Leaving the name in
            # the config would let the compliance report name a profile that
            # contributed no rules -- protection that never ran.
            config.pop("privacy_profile", None)

        if profile_rules:
            # User rules override profile rules
            user_rules = config.get("phi_tags", {})
            profile_rules.update(user_rules)
            config["phi_tags"] = profile_rules

    return config


class ConfigLoader:
    """
    Loads and validates configuration files for the Isocenter system.

    This class provides static methods to parse unified YAML configuration files (v2.0),
    legacy configuration formats, and PHI tag definitions. It handles configuration
    validation, normalization, and file I/O operations.

    Supports multiple configuration formats:
    - Unified v2.0 YAML configs with PHI tags, machine rules, and date jitter settings
    - Legacy machine rule configurations
    - PHI tag definitions (from files or internal defaults)

    The class also provides utility methods for filename sanitization and YAML parsing.
    """

    @staticmethod
    def load_unified_config(
            filepath: str) -> tuple[Dict[str, Any], List[Dict[str, Any]],
                                    Dict[str, Any], bool, Optional[str]]:
        """
        Parses the unified YAML config (v2.0).

        Extracts the core configuration components: PHI tags, machine rules,
        date jitter settings, and global flags.

        Args:
            filepath (str): Path to the config file.

        Returns:
            tuple: (phi_tags, machine_rules, date_jitter_config,
            remove_private_tags, privacy_profile). The last element is the
            name of the profile whose rules were merged, or None -- an
            unknown reference resolves to None rather than to its own name,
            because it contributed nothing.
        """
        # Call the top-level loader which handles YAML, Legacy List, and Privacy Profiles
        data = load_unified_config(filepath)

        phi_tags = data.get("phi_tags", {})
        # Support 'machines' (v2) or 'machine_rules' (legacy internal)
        machine_rules = data.get("machines", data.get("machine_rules", []))

        # Date Jitter Normalization
        dj = data.get("date_jitter", {"min_days": -365, "max_days": -1})
        if isinstance(dj, int):
            # Legacy support or user provided int. Convert to fixed shift.
            date_jitter_config = {"min_days": dj, "max_days": dj}
        else:
            date_jitter_config = dj

        remove_private_tags = data.get("remove_private_tags", True)

        # Validate machines
        for i, rule in enumerate(machine_rules):
            ConfigLoader._validate_rule(rule, i)

        return (phi_tags, machine_rules, date_jitter_config,
                remove_private_tags, data.get("privacy_profile"))

    @staticmethod
    def load_redaction_rules(filepath: str) -> List[Dict[str, Any]]:
        """
        Legacy/Convenience support for loading only Machine Rules.

        Use this if you only need the 'machines' list from a unified config,
        or an old-style legacy config file.

        Args:
            filepath (str): Path to the config file.

        Returns:
            List[Dict[str, Any]]: List of validated machine rule dictionaries.
        """
        data = ConfigLoader._load_yaml(filepath)

        rules = []

        if "machines" in data:
            rules = data["machines"]  # v1 or v2
        else:
            get_logger().warning("Config Warning: Could not find 'machines' list.")

        for i, rule in enumerate(rules):
            ConfigLoader._validate_rule(rule, i)

        return rules

    @staticmethod
    def load_phi_config(filepath: str = None) -> Dict[str, str]:
        """
        Legacy/Convenience support for loading only PHI Tags.

        Arg:
            filepath (str, optional): Path to config file. If None, returns
                a copy of the floor policy, `profiles.FLOOR_POLICY`.

        Returns:
            Dict: Mapping of tags to configuration (action/name).
        """
        if filepath:
            data = ConfigLoader._load_yaml(filepath)

            # Support v2 unified file used as simple PHI config
            if "phi_tags" in data:
                return data["phi_tags"]
            return data.get("phi_tags", data)  # Fallback to assumes root dict is tags if no key
        # The default policy is the floor a bare session applies (#495).
        # It was `resources/phi_tags.json` -- six name-only tags, every
        # one of them already in the basic profile -- which only this
        # arm, the scaffold's tag names and the report's rule count ever
        # read, while `audit()` on a bare session scanned against `{}`:
        # the report named six rules the scan never ran. A copy, because
        # `PhiInspector` normalizes what it is handed and a caller may
        # edit it.
        return copy.deepcopy(FLOOR_POLICY)

    @staticmethod
    def clean_filename(filename: str) -> str:
        """
        Sanitizes a string to be safe for use as a filename.

        Replaces spaces with underscores and removes non-alphanumeric characters
        (except key delimiters like dash/dot).
        """
        # import re  <-- Removed

        s = str(filename).strip().replace(" ", "_")
        return re.sub(r'(?u)[^-\w.]', '', s)

    @staticmethod
    def _load_yaml(filepath: str) -> Dict[str, Any]:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r', encoding="utf-8") as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format in {filepath}: {e}") from e

    @staticmethod
    def _validate_rule(rule: Dict[str, Any], index: int):
        sn = rule.get("serial_number")
        if not sn:
            raise ValueError(f"Rule #{index}: Missing 'serial_number'.")

        zones = rule.get("redaction_zones", [])
        if not isinstance(zones, list):
            raise ValueError(f"Rule #{index} ({sn}): 'redaction_zones' must be a list.")

        for z_idx, zone in enumerate(zones):
            if isinstance(zone, list):
                roi = zone
            elif isinstance(zone, dict):
                roi = zone.get("roi")
            else:
                raise ValueError(
                    f"Rule #{index} ({sn}), Zone #{z_idx}: Invalid zone format (must be list or dict).")

            if not roi or not isinstance(roi, list) or len(roi) != 4:
                raise ValueError(
                    f"Rule #{index} ({sn}), Zone #{z_idx}: ROI must be a list of 4 integers.")

            r1, r2, c1, c2 = roi
            if any(x < 0 for x in roi):
                raise ValueError(
                    f"Rule #{index} ({sn}), Zone #{z_idx}: ROI values must be non-negative.")

            if r1 > r2 or c1 > c2:
                raise ValueError(
                    f"Rule #{index} ({sn}), Zone #{z_idx}: Invalid ROI logic (Start > End).")
