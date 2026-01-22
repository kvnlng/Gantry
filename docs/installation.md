# Installation

Gantry requires **Python 3.9+**.

```bash
pip install "git+https://github.com/kvnlng/Gantry.git"
```

!!! note
    The `imagecodecs` dependency is included and strongly recommended for handling JPEG Lossless and other compressed Transfer Syntaxes.

## System Requirements

Gantry's parallel processing engine is designed to maximize CPU utilization. However, heavy operations like JPEG 2000 compression require significant memory per worker.

- **Memory**: Gantry is memory-intensive during specific operations (e.g., Pixel Redaction, J2K Export).
  - **Minimum**: 2GB RAM per vCPU.
  - **Recommended (Heavy Workloads)**: 8GB RAM per vCPU (e.g., for massive multi-frame J2K compression).
- **Concurrency**: By default, Gantry uses all available cores (`1:1` ratio). Use `GANTRY_MAX_WORKERS` env var to limit this if OOM occurs.
