"""Sphinx configuration for the public library documentation."""

import json
import sys
from pathlib import Path

from setuptools_scm import get_version

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docs"))
project = "Rulebricks Spark"
author = "Rulebricks"
copyright = "2026, Prefix Software, Inc."
release = get_version(root=str(ROOT), dist_name="rulebricks-spark")

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
html_css_files = ["fonts/public-sans.css", "fonts/jetbrains-mono.css", "custom.css"]
pygments_style = "_syntax.RulebricksLight"
pygments_dark_style = "_syntax.RulebricksDark"
# Match rulebricks-documentation's theme.config.tsx and styles/globals.css.
html_theme_options = {
    "source_repository": "https://github.com/rulebricks/spark/",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {
        "font-stack": '"Public Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
        "font-stack--headings": '"Public Sans", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
        "font-stack--monospace": '"JetBrains Mono", "SFMono-Regular", Consolas, monospace',
        "color-foreground-primary": "#111111",
        "color-foreground-secondary": "#4b5563",
        "color-foreground-muted": "#6b7280",
        "color-foreground-border": "#d1d5db",
        "color-background-primary": "#fcfcf9",
        "color-background-secondary": "#f5f5f2",
        "color-background-hover": "#eeeeeb",
        "color-background-hover--transparent": "#eeeeeb00",
        "color-background-border": "#e5e7eb",
        "color-background-item": "#d1d5db",
        "color-brand-primary": "hsl(104, 60%, 45%)",
        "color-brand-content": "#3b8321",
        "color-brand-visited": "var(--color-brand-content)",
        "color-sidebar-background": "var(--color-background-primary)",
        "color-sidebar-link-text--top-level": "var(--color-foreground-secondary)",
        "color-sidebar-item-background--current": "hsl(104, 60%, 94%)",
        "color-highlighted-background": "hsl(104, 60%, 94%)",
        "color-highlight-on-target": "hsl(104, 60%, 94%)",
        "color-api-name": "var(--color-brand-content)",
        "color-api-pre-name": "var(--color-brand-content)",
        "color-admonition-title--note": "var(--color-brand-content)",
        "color-admonition-title-background--note": "hsl(104, 60%, 94%)",
        "color-admonition-title--seealso": "var(--color-brand-content)",
        "color-admonition-title-background--seealso": "hsl(104, 60%, 94%)",
    },
    "dark_css_variables": {
        "color-foreground-primary": "#b7c1b7",
        "color-foreground-secondary": "#9ca3af",
        "color-foreground-muted": "#9ca3af",
        "color-foreground-border": "#444444",
        "color-background-primary": "#111111",
        "color-background-secondary": "#1a1a1a",
        "color-background-hover": "#272727",
        "color-background-hover--transparent": "#27272700",
        "color-background-border": "#333333",
        "color-background-item": "#444444",
        "color-brand-primary": "hsl(104, 60%, 55%)",
        "color-brand-content": "hsl(104, 60%, 55%)",
        "color-brand-visited": "var(--color-brand-content)",
        "color-sidebar-item-background--current": "hsla(104, 60%, 66%, 0.1)",
        "color-highlighted-background": "hsl(104, 35%, 18%)",
        "color-highlight-on-target": "hsl(104, 35%, 18%)",
        "color-admonition-background": "#1a1a1a",
        "color-admonition-title-background--note": "hsla(104, 60%, 66%, 0.1)",
        "color-admonition-title-background--seealso": "hsla(104, 60%, 66%, 0.1)",
        "color-card-background": "#1a1a1a",
    },
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
