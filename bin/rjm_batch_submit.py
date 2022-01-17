#!/usr/bin/env python
"""
This script will...

"""
import argparse


def main():
    # command line arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-s', '--script', required=True, help='Name of Slurm script')
    args = parser.parse_args()




if __name__ == "__main__":
    main()
