#!/usr/bin/env python

import os
import sys
import subprocess

import defopt


def get_python_license():
    # get python license
    verinfo = sys.version_info
    shortver = f"python{verinfo.major}.{verinfo.minor}"
    license_path = os.path.join(sys.base_prefix, "lib", shortver, "LICENSE.txt")
    assert os.path.exists(license_path)
    with open(license_path) as fh:
        python_license = fh.read()

    return python_license


def get_pip_licenses():
    # dump licenses from pip
    pip_licenses = subprocess.check_output(['pip-licenses', '-f', 'plain-vertical', '-l'], stderr=subprocess.PIPE, text=True)

    return pip_licenses


def main(output_file: str):
    """
    Dump Python and pip installed package licenses to a file

    :param output_file: The file to store the licenses in
    """
    # get the licenses
    python_license = get_python_license()
    pip_licenses = get_pip_licenses()

    # write to file
    with open(output_file, "w") as fh:
        fh.write("=========== Python license\n\n")
        fh.write(python_license)
        fh.write("\n=========== Pip licenses\n\n")
        fh.write(pip_licenses)


if __name__ == "__main__":
    defopt.run(main)
