
import os
import codecs

from cx_Freeze import setup, Executable


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


script_dir = os.path.join(os.path.dirname(__file__), os.pardir, "scripts")
rjm_batch_submit = os.path.join(script_dir, "rjm_batch_submit.py")
print(rjm_batch_submit)
rjm_batch_wait = os.path.join(script_dir, "rjm_batch_wait.py")
print(rjm_batch_wait)

build_exe_options = {
    "packages": [
        "websockets.legacy.client",
    ]
}

setup(
    name='RemoteJobManager',
    version=get_version("../../rjm/__init__.py"),
    description='Remote Job Manager',
    options={"build_exe": build_exe_options},
    executables=[
        Executable(
            script=rjm_batch_submit,
            target_name='rjm_batch_submit',
        ),
        Executable(
            script=rjm_batch_wait,
            target_name='rjm_batch_wait',
        ),
    ],
)
