import os
import sys

sys.path.insert(0, os.path.abspath('../..'))

import sphinx_rtd_theme
from pgcom import __version__

project = 'pgcom'
copyright = '2020, viktorsapozhok'
author = 'viktorsapozhok'
user = 'viktorsapozhok'

version = __version__
release = __version__

highlight_language = 'python'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
]

# Add any paths that contain templates here, relative to this directory.
# Uses the same 'sphinx.ext.autosummary' template as the statsmodels package:
# see https://github.com/statsmodels/statsmodels
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["*/autosummary/*.rst"]

# Enable 'autosummary'
autosummary_generate = True
autoclass_content = "both"

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = False

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'
#pygments_style = "default"

language = "en"
#language = None
autodoc_inherit_docstrings = False
autodoc_member_order = 'bysource'
add_module_names = False

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_rtd_theme'

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    'navigation_depth': 3,
    'includehidden': False,
}

html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

html_context = {
    'display_github': True,
    'github_user': user,
    'github_repo': project,
    'github_version': 'master',
    'conf_py_path': '/docs/source/',
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []
