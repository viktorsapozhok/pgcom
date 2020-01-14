# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))
import warnings

warnings.simplefilter('ignore', DeprecationWarning)

import sphinx_rtd_theme  # NOQA: F401


# -- Project information -----------------------------------------------------

project = 'pgcom'
copyright = '2019, viktorsapozhok'
author = 'viktorsapozhok'

# The full version, including alpha/beta/rc tags
try:
    from pgcom import __version__ as version
except ImportError:
    pass
else:
    release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # Support for NumPy and Google style docstrings
    'sphinx.ext.viewcode',  # Add links to highlighted source code
    'sphinx.ext.mathjax',  # Render math via JavaScript
    'sphinx_autodoc_typehints',  # Moves type hints from signature to docs
]

# Add any paths that contain templates here, relative to this directory.
templates_path = []

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# Disable documentation inheritance so as to avoid inheriting docstrings in a
# different format.
autodoc_inherit_docstrings = False

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = False


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'
# html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []
