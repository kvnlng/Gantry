from dataclasses import dataclass, asdict
from typing import List, Optional, Protocol
import json
import os
import datetime
from .logger import get_logger

@dataclass
class ManifestItem:
    """
    Represents a single exported file or instance in the manifest.
    """
    patient_id: str
    study_instance_uid: str
    series_instance_uid: str
    sop_instance_uid: str
    
    # File details
    file_path: str = ""
    file_size_bytes: int = 0
    
    # Optional metadata
    modality: str = ""
    manufacturer: str = ""
    model_name: str = ""
    
    # Processing details
    anonymized: bool = True


@dataclass
class Manifest:
    """
    Collection of manifest items.
    """
    generated_at: str
    items: List[ManifestItem]
    project_name: str = "Gantry Session"
    total_files: int = 0
    total_size_bytes: int = 0
    
    def to_dict(self):
        return {
            "generated_at": self.generated_at,
            "project_name": self.project_name,
            "total_files": len(self.items),
            "total_size_bytes": sum(i.file_size_bytes for i in self.items),
            "items": [asdict(i) for i in self.items]
        }

class ManifestRenderer(Protocol):
    def render(self, manifest: Manifest, output_path: str) -> None:
        ...

class JSONManifestRenderer:
    def render(self, manifest: Manifest, output_path: str) -> None:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(manifest.to_dict(), f, indent=2)

class HTMLManifestRenderer:
    def render(self, manifest: Manifest, output_path: str) -> None:
        # Basic accessible HTML table
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gantry Manifest - {manifest.project_name}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 2rem; color: #333; }}
        h1 {{ margin-bottom: 0.5rem; }}
        .meta {{ color: #666; margin-bottom: 2rem; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
        th, td {{ text-align: left; padding: 0.75rem; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f8f9fa; font-weight: 600; position: sticky; top: 0; }}
        tr:hover {{ background-color: #f5f5f5; }}
        .badge {{ padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }}
        .badge-success {{ background-color: #d4edda; color: #155724; }}
    </style>
</head>
<body>
    <h1>Data Manifest</h1>
    <div class="meta">
        <strong>Project:</strong> {manifest.project_name} &bull;
        <strong>Generated:</strong> {manifest.generated_at} &bull;
        <strong>Files:</strong> {len(manifest.items)}
    </div>

    <table>
        <thead>
            <tr>
                <th>Patient ID</th>
                <th>Study UID</th>
                <th>Series UID</th>
                <th>Modality</th>
                <th>Manufacturer</th>
                <th>Model</th>
                <th>SOP Instance UID</th>
                <th>File Path</th>
            </tr>
        </thead>
        <tbody>
"""
        for item in manifest.items:
            html += f"""
            <tr>
                <td>{item.patient_id}</td>
                <td>{item.study_instance_uid}</td>
                <td>{item.series_instance_uid}</td>
                <td>{item.modality}</td>
                <td>{item.manufacturer}</td>
                <td>{item.model_name}</td>
                <td>{item.sop_instance_uid}</td>
                <td><code>{item.file_path}</code></td>
            </tr>
"""
        html += """
        </tbody>
    </table>
</body>
</html>
"""
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)

def generate_manifest_file(manifest: Manifest, output_path: str, format: str = "html"):
    if format.lower() == "json":
        renderer = JSONManifestRenderer()
    elif format.lower() == "html":
        renderer = HTMLManifestRenderer()
    else:
        raise ValueError(f"Unsupported manifest format: {format}")
        
    renderer.render(manifest, output_path)
