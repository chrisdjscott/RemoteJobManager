
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

    config_helper.do_configuration()


if __name__ == "__main__":
    configure()
