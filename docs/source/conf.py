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
pygments_style = 'sphinx'

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.mathjax',
    'sphinx_autodoc_typehints',
]

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
language = None
exclude_patterns = []
autodoc_inherit_docstrings = False
add_module_names = False
html_theme = 'sphinx_rtd_theme'
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]

html_context = {
    'display_github': True,
    'github_user': user,
    'github_repo': project,
    'github_version': 'master',
    'conf_py_path': '/docs/source/',
}

html_static_path = ['_static']
