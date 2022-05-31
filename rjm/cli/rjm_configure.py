
import os
import sys
import shutil
import argparse

from rjm import __version__
from rjm.utils import backup_file
from rjm import config as config_helper


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Walk through the configuration of RJM")
    parser.add_argument("-e", "--export-config", help="Export current config to the given file (e.g. to transfer config to another machine)")
    parser.add_argument("-i", "--import-config", help="Import config from the given file, overwriting the current config")
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)

    return parser


def configure(args_in=None):
    """
    Run through configuration steps

    """
    # parse and check arguments
    parser = make_parser()
    args = parser.parse_args(args_in)
    if args.export_config is not None and args.import_config is not None:
        print("ERROR: '--export-config' and '--import-config' are mutually exclusive")
        sys.exit(1)

    # export config file
    if args.export_config is not None:
        if os.path.exists(config_helper.CONFIG_FILE_LOCATION):
            shutil.copy(config_helper.CONFIG_FILE_LOCATION, args.export_config)
        else:
            print("ERROR: cannot export config because no config file exists")
            sys.exit(1)

    # import config file
    elif args.import_config is not None:
        if os.path.isfile(args.import_config):
            # create the config directory if it doesn't exist
            config_dir = os.path.dirname(config_helper.CONFIG_FILE_LOCATION)
            if not os.path.exists(config_dir):
                os.makedirs(config_dir, mode=0o700)

            # backup the current config if there is one
            elif os.path.isfile(config_helper.CONFIG_FILE_LOCATION):
                bkp_file = backup_file(config_helper.CONFIG_FILE_LOCATION)
                print(f"Backed up current config to: {bkp_file}")

            # copy the new config file
            shutil.copy(args.import_config, config_helper.CONFIG_FILE_LOCATION)

        else:
            print("ERROR: provided file does not exist")
            sys.exit(1)

    # run through the configuration
    else:
        try:
            config_helper.do_configuration()
        except Exception as exc:
            print("ERROR: rjm_configure failed with exception:")
            print(repr(exc))
            sys.exit(1)


if __name__ == "__main__":
    configure()
