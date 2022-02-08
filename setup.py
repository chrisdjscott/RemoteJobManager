from setuptools import setup, find_packages
import codecs
import os.path


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setup(
    name="RemoteJobManager",
    version=get_version("rjm/__init__.py"),
    description="Manage jobs running remotely on a cluster",
    url="https://github.com/chrisdjscott/RemoteJobManager",
    author="Chris Scott",
    author_email="chris.scott@nesi.org.nz",
    license="MIT",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'rjm_batch_submit = rjm.cli:batch_submit',
            'rjm_batch_wait = rjm.cli:batch_wait',
        ],
        'pyinstaller40': [
            'hook-dirs = rjm.__pyinstaller:get_hook_dirs',
            'tests = rjm.__pyinstaller:get_PyInstaller_tests',
        ],
    },
    install_requires=[
        "requests",
        "fair-research-login",
        "globus-sdk",
        "funcx==0.3.5",
    ],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
