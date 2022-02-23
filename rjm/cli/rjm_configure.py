
import sys
import argparse

from rjm import __version__
from rjm import config as config_helper


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Walk through the configuration of RJM")
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)

    return parser


def configure():
    """
    Run through configuration steps

    """
    parser = make_parser()
    parser.parse_args()

    try:
        config_helper.do_configuration()
    except Exception as exc:
        print("ERROR: rjm_configure failed with exception:")
        print(repr(exc))
        sys.exit(1)


if __name__ == "__main__":
    configure()
