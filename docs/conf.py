# Configuration file for the Sphinx documentation builder.

# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys

import click

sys.path.insert(0, os.path.abspath(".."))


# -- Project information -----------------------------------------------------

project = "Forecasting Platform"
author = "Oliver Wyman"

# The full version, including alpha/beta/rc tags
release = "Beta"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_click.ext",  # https://github.com/click-contrib/sphinx-click
    "sphinx.ext.autosectionlabel",
]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# HTML output options: https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"  # see https://sphinx-rtd-theme.readthedocs.io/en/stable/index.html
html_show_copyright = False
html_show_sphinx = False

# Napoleon options: https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html#configuration

napoleon_google_docstring = True  # forecast_platform uses google-style doc-strings for better Sphinx/Napoleon support
napoleon_numpy_docstring = True  # owforecasting package currently uses numpy-style doc-strings

# Autodoc options: https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

autodoc_mock_imports = [
    "pandas",
    "h2o",
]

autodoc_typehints = "description"

# Autosummary options: https://www.sphinx-doc.org/en/master/usage/extensions/autosummary.html

autosummary_generate = True  # Note: This works in combination with `:toctree: _stubs`

# Intersphinx options: https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html#configuration

intersphinx_mapping = {
    "python": ("https://docs.python.org/3.8", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "click": ("https://click.palletsprojects.com/en/7.x/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/13/", "https://docs.sqlalchemy.org/en/13/objects.inv"),
    "h2o": (
        "https://h2o-release.s3.amazonaws.com/h2o/rel-yu/3/docs-website/h2o-py/docs/",
        "https://h2o-release.s3.amazonaws.com/h2o/rel-yu/3/docs-website/h2o-py/docs/objects.inv",
    ),
}

# The following is a workaround for avoiding truncation of help strings by sphinx-click.
# This overrides the default "limit" of 45 characters for help strings in the command overview.
# Relevant upstream line of code: https://github.com/click-contrib/sphinx-click/blob/2.3.2/sphinx_click/ext.py#L194


click_short_help_original = click.Command.get_short_help_str


def patched_get_short_help_str(self: click.Command, limit: int = 500) -> str:
    """Patched ``get_short_help_str`` method to override the default character limit when generating documentation."""
    return click_short_help_original(self, limit=limit)


click.Command.get_short_help_str = patched_get_short_help_str  # type: ignore
