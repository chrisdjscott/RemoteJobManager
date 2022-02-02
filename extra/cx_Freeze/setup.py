
import os

from cx_Freeze import setup, Executable


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
    version='0.0.1',
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
