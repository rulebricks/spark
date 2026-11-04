# Develop and document the library

The documentation is built with Sphinx and Furo. Guides are Markdown, public API
signatures are read from the Python source with AutoAPI, and notebooks are rendered
from `examples/` with MyST-NB.

The docs use Public Sans for text and headings, and JetBrains Mono for code.
Font files and their licenses are bundled in `docs/_static/fonts/`; Furo's font
settings are in `docs/conf.py`. Builds and readers do not need a font CDN.

Colors follow `rulebricks-documentation`'s `theme.config.tsx` and
`styles/globals.css`: off-white and charcoal backgrounds with green accents.
The light and dark palettes are in `docs/conf.py`; `docs/_syntax.py` maps the
reference docs' syntax colors to Sphinx. Link and comment colors use darker or
lighter shades where needed to keep text readable.

## Releasing

The Git tag is the release version. There is no separate source version to update.

1. Commit and push the changes you want to release, including any workflow fixes.
2. Create and push a version tag on that commit, such as `v0.2.2` or `0.2.2`.
3. The release workflow runs CI, builds the wheel and source archive, checks both
   against the tag, and publishes them to PyPI.

Hatch derives versions from Git for local builds too. Commits after a release tag
receive a development version. Installed `rulebricks_spark.__version__` comes from
the package metadata, so it matches the installed distribution. Reinstall an
editable checkout after changing tags or commits to refresh its version.

A failed run still uses the code and workflow at its original commit when retried.
If you fix the workflow after creating a tag, use a new tag on the corrected
commit, or deliberately recreate the failed tag there if it has not been released.
Simply rerunning the old job does not include the fix. Published PyPI filenames
cannot be reused; do not enable `skip-existing` to hide a version mismatch.
