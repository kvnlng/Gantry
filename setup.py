from setuptools import setup, find_packages

setup(
    name="gantry",
    version="0.1.0",
    description="A Python DICOM Object Model and Redaction Toolkit",
    author="Kevin Long",
    packages=find_packages(),
    install_requires=[
        "pydicom>=2.4.0",
        "numpy>=1.20.0",
        "pylibjpeg>=2.1.0"
    ],
    python_requires=">=3.9",
)