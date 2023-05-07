"""
The Remote Job Manager python package

"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("RemoteJobManager")
except PackageNotFoundError:
    # package is not installed
    __version__ = "unknown"
