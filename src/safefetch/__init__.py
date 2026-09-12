"""HTTP calls that stay inside rate limits, retry sensibly, and back off when a service is failing."""

from importlib.metadata import version

__version__ = version("safefetch")

__all__ = ["__version__"]