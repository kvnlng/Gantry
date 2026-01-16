# Gantry

**A Python DICOM Object Model and Redaction Toolkit.**

![Gantry](images/IMG_0653.jpeg)

Gantry provides a high-performance, object-oriented interface for managing, analyzing, and de-identifying DICOM datasets. It is designed for large-scale ingestion, precise pixel redaction, and strict PHI compliance.

## Features

- **Object-Oriented API**: Work with `Patient`, `Study`, `Series`, and `Instance` objects directly.
- **Persistent Sessions**: All metadata is indexed in a SQLite database, allowing you to pause/resume large jobs and providing an audit trail.
- **Parallel Processing**: Multi-process ingestion and export for maximum throughput.
- **Robust Redaction**:
  - **Metadata**: Configurable tag removal, replacement, and shifting.
  - **Pixel Data**: Machine-specific redaction zones (ROI) to scrub burned-in PHI.
  - **Reversibility**: Optional cryptographic identity preservation.
- **Codecs**: Robust support for JPEG Lossless, JPEG 2000, and other compressed formats via `imagecodecs`.
- **Free-threaded Python Ready**: Fully compatible with Python 3.13t+ (no-GIL) for true parallelism.
- **Deep Memory Management**: Automatic pixel offloading allows processing datasets far exceeding available RAM.

## Performance

Gantry is designed for massive scale. Recent stress tests verify robust linear scaling on datasets up to 100GB.

![Benchmark Scaling](docs_site/images/benchmark_scaling.png)

### 100GB Scalability Test

- **Input**: 101,000 files (50GB Single-Frame + 50GB Multi-Frame).
- **Import Speed**: ~14 seconds (Index-only ingestion).
- **Export Speed**: ~79 seconds (Streaming Write).
- **Memory**: Peaks at 5.4GB, stable regardless of dataset size.

The architecture uses O(1) memory streaming, ensuring it never runs out of RAM even when processing terabytes of data.

#### Micro-Benchmarks (Metadata Operations)

| Operation            | Scale             | Time (Mac M3 Max) | Throughput     |
|----------------------|-------------------|-------------------|----------------|
| **Identity Locking** | 100,000 Instances | ~0.13 s           | **769k / sec** |
| **Persist Findings** | 100,000 Issues    | ~0.13 s           | **770k / sec** |

## Architecture

Gantry acts as a smart indexing layer over your raw DICOM files. It does *not* modify your original data. Instead, it builds a lightweight metadata index (SQLite) and exposes a clean Python Object Model for manipulation.