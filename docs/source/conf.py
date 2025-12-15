import os
import sys
# Point to the root directory so Sphinx can find 'gantry'
sys.path.insert(0, os.path.abspath('../..'))

project = 'Gantry'
copyright = '2025, Kevin Long'
author = 'Kevin Long'
release = '0.1.0'

extensions = [
    'sphinx.ext.autodoc',      # Auto-generate docs from code
    'sphinx.ext.napoleon',     # Support Google-style docstrings
    'sphinx.ext.viewcode',     # Add links to source code
    'sphinx.ext.githubpages',  # Create .nojekyll file for GitHub
    'myst_parser',             # Parse Markdown (README)
]

templates_path = ['_templates']
exclude_patterns = []

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
