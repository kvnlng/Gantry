from setuptools import setup, find_packages

setup(
    name="gantry",
    version="0.4.0",
    description="A Python DICOM Object Model and Redaction Toolkit",
    author="Kevin Long",
    packages=find_packages(),
    install_requires=[
        "pydicom>=2.4.0",
        "numpy>=1.20.0",
        "tqdm>=4.65.0",
        "cryptography>=41.0.0",
        "pandas>=2.0.0",
        # "pylibjpeg==2.1.0"
    ],
    python_requires=">=3.9",
    extras_require={
        "docs": [
            "sphinx>=7.0.0",
            "sphinx_rtd_theme>=2.0.0",
            "myst_parser>=2.0.0",
        ]
    }
)
