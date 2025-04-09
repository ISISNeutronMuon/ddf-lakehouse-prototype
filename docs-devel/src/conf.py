# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "ISIS Accelerator Data"
copyright = "2025, ISIS Computing Division"
author = "ISIS Computing Division"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

exclude_patterns = ["./common_links.rst"]
extensions = ["myst_parser"]
master_doc = "index"
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
templates_path = ["_templates"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_css_files = ["custom.css"]
html_theme = "sphinx_book_theme"
html_static_path = ["../../_static_shared"]
html_logo = "../../_static_shared/logo.svg"
html_theme_options = {"show_prev_next": False}
