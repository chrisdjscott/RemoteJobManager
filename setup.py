import glob
from setuptools import setup, find_packages

setup(
    name="Remote Job Manager",
    version="0.0.1",
    description="Manage jobs running remotely on a cluster",
    url="https://github.com/chrisdjscott/rjm",
    author="Chris Scott",
    author_email="chris.scott@nesi.org.nz",
    license="MIT",
    packages=find_packages(),
    scripts=glob.glob("bin/*"),
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
