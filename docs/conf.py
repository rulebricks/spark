"""Sphinx configuration for the public library documentation."""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
project = "Rulebricks Spark"
author = "Rulebricks"
copyright = "2026, Prefix Software, Inc."
release = re.search(
    r'__version__ = "([^"]+)"',
    (ROOT / "src/rulebricks_spark/_version.py").read_text(),
).group(1)

extensions = ["myst_nb", "sphinx.ext.autodoc", "sphinx.ext.napoleon", "autoapi.extension"]
nb_execution_mode = "off"
myst_heading_anchors = 3
myst_enable_extensions = ["colon_fence"]
exclude_patterns = ["_build", "requirements.txt"]

# Static analysis keeps the documentation build independent of the Spark runtime.
autoapi_dirs = [str(ROOT / "src/rulebricks_spark")]
autoapi_generate_api_docs = False
autoapi_add_toctree_entry = False
autoapi_options = ["members", "undoc-members", "imported-members"]
autodoc_typehints = "signature"
python_maximum_signature_line_length = 80
python_use_unqualified_type_names = True

html_theme = "furo"
html_title = "Rulebricks Spark"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_theme_options = {
    "source_repository": "https://github.com/rulebricks/spark/",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {"color-brand-primary": "#2461b2", "color-brand-content": "#245a9b"},
    "dark_css_variables": {"color-brand-primary": "#8bbdff", "color-brand-content": "#8bbdff"},
}


def prepare_notebooks(app):
    """Render the examples from their single authoring location."""
    destination = Path(app.srcdir) / "_notebooks"
    destination.mkdir(exist_ok=True)
    for previous in destination.glob("*.ipynb"):
        previous.unlink()
    for source in sorted((ROOT / "examples").glob("*.ipynb")):
        notebook = json.loads(source.read_text())
        for cell in notebook["cells"]:
            if cell["cell_type"] == "markdown":
                cell["source"] = [line.replace("../docs/", "../") for line in cell["source"]]
        notebook["cells"][0]["source"].insert(
            1, f"\n{{download}}`Download this notebook <../../examples/{source.name}>`\n"
        )
        (destination / source.name).write_text(json.dumps(notebook, indent=1) + "\n")


def notebook_source_links(app, pagename, templatename, context, doctree):
    if pagename.startswith("_notebooks/"):
        source = f"examples/{Path(pagename).name}.ipynb"
        context["theme_source_view_link"] = (
            f"https://github.com/rulebricks/spark/blob/main/{source}"
        )
        context["theme_source_edit_link"] = (
            f"https://github.com/rulebricks/spark/edit/main/{source}"
        )


def setup(app):
    app.connect("builder-inited", prepare_notebooks)
    app.connect("html-page-context", notebook_source_links)
