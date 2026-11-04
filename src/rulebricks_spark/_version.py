"""Read the version recorded when the package was built or installed."""

from importlib.metadata import version

__version__ = version("rulebricks-spark")
