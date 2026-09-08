# Develop and document the library

The documentation is built with Sphinx and Furo. Guides are Markdown, public API
signatures are read from the Python source with AutoAPI, and notebooks are rendered
from `examples/` with MyST-NB. The build never imports Spark or executes a notebook,
so it needs neither Java nor Rulebricks credentials.

## Build the site

Create a separate Python 3.13 environment and install the documentation requirements:

```bash
python3.13 -m venv .venv-docs
.venv-docs/bin/python -m pip install -r docs/requirements.txt
.venv-docs/bin/python -m sphinx -b html -W --keep-going docs docs/_build/html
.venv-docs/bin/python -m http.server 8000 --directory docs/_build/html
```

The direct documentation dependencies are pinned in `docs/requirements.txt`.
The root `.readthedocs.yaml` selects Ubuntu 24.04, Python 3.13, and this requirements
file, and treats Sphinx warnings as failures. Import the repository into Read the
Docs and select the desired branch to enable hosted builds. Adding the configuration
to a checkout does not by itself register or publish a hosted project.

Edit notebook sources in `examples/`; `docs/_notebooks/` is generated during the
build and should not be committed. Keep execution outputs and credentials out of
the sources. The rendered site provides notebook downloads without running their
external calls as part of a documentation build.

## Check a change

Library tests exercise the generated SDK through controlled HTTP fixtures and
classic Spark. Notebook execution additionally exercises Delta output, streaming
checkpoints, and the public examples. Those local checks establish behavior at
that boundary; deployment throughput and managed runtime compatibility still need
representative testing on the target system.

```bash
python -m pytest -q
python -m ruff check src tests examples scripts
python -m mypy src/rulebricks_spark
python scripts/validate_notebooks.py
python scripts/validate_docs.py
```

Keep guides focused on the reader's pipeline and move parameter details into the
reference. When a public signature changes, update the explanation of its behavior
and run the documentation build so the generated reference and links stay current.
