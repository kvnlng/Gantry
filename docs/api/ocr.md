# Intelligent OCR API

## Zone Discovery

::: gantry.discovery.ZoneDiscoverer
    handler: python
    options:
      show_root_heading: true
      show_source: true
      members:
        - discover_zones

## Verification

::: gantry.verification.RedactionVerifier
    handler: python
    options:
      show_root_heading: true

## Automation

::: gantry.automation.ConfigAutomator
    handler: python
    options:
      show_root_heading: true
      members:
        - suggest_config_updates

## Pixel Analysis

::: gantry.pixel_analysis
    handler: python
    options:
      show_root_heading: true
      members:
        - analyze_pixels
        - detect_text_regions
