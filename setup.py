from setuptools import setup, find_packages

setup(
    name="gantry",
    version="0.5.3",
    description="A Python DICOM Object Model and Redaction Toolkit",
    author="Kevin Long",
    packages=find_packages(),
    install_requires=[
        "pydicom>=2.4.0",
        "numpy>=1.20.0",
        "tqdm>=4.65.0",
        "cryptography>=41.0.0",
        "pandas>=2.0.0",
        "PyYAML>=6.0",
        "pillow>=10.0.0",
        "imagecodecs>=2023.0.0"
    ],
    python_requires=">=3.9",
    extras_require={
        "docs": [
            "mkdocs>=1.5.0",
            "mkdocs-material>=9.0.0",
            "mkdocstrings[python]>=0.20.0",
            "mkdocs-awesome-pages-plugin>=2.8.0"
        ]
    }
)
